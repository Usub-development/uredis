#include "uredis/RedisBus.h"
#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis
{
    RedisBus::RedisBus(Config cfg)
        : cfg_(std::move(cfg))
    {
    }

    void RedisBus::notify_error(const RedisError& err) const
    {
        if (this->cfg_.on_error)
            this->cfg_.on_error(err);
    }

    void RedisBus::notify_reconnect() const
    {
        if (this->cfg_.on_reconnect)
            this->cfg_.on_reconnect();
    }

    task::Awaitable<RedisResult<void>> RedisBus::ensure_connected_locked()
    {
        if (this->connected_ && this->pub_client_ && this->sub_client_)
        {
            co_return RedisResult<void>{};
        }

        this->pub_client_.reset();
        this->sub_client_.reset();
        this->connected_ = false;

        this->pub_client_ = std::make_shared<RedisClient>(this->cfg_.redis);
        this->sub_client_ = std::make_shared<RedisSubscriber>(this->cfg_.redis);

        auto cp = co_await this->pub_client_->connect();
        if (!cp)
        {
            const auto& err = cp.error();
#ifdef UREDIS_LOGS
            usub::ulog::error("RedisBus::ensure_connected_locked: pub connect failed: {}", err.message);
#endif
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        auto cs = co_await this->sub_client_->connect();
        if (!cs)
        {
            const auto& err = cs.error();
#ifdef UREDIS_LOGS
            usub::ulog::error("RedisBus::ensure_connected_locked: sub connect failed: {}", err.message);
#endif
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        this->connected_ = true;
#ifdef UREDIS_LOGS
        usub::ulog::info("RedisBus: connected pub+sub");
#endif

        auto r = co_await this->resubscribe_all_locked();
        if (!r)
        {
            const auto& err = r.error();
#ifdef UREDIS_LOGS
            usub::ulog::info("RedisBus::ensure_connected_locked: resubscribe_all failed: {}", err.message);
#endif
            this->notify_error(err);
        }

        this->notify_reconnect();
        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void>> RedisBus::resubscribe_all_locked()
    {
        if (!this->connected_ || !this->sub_client_)
        {
            RedisError err{RedisErrorCategory::Io, "RedisBus: not connected in resubscribe_all"};
            co_return std::unexpected(err);
        }

        for (auto& [ch, cb] : this->desired_channels_)
        {
            auto r = co_await this->sub_client_->subscribe(ch, cb);
            if (!r)
            {
                const auto& err = r.error();
#ifdef UREDIS_LOGS
                usub::ulog::info("RedisBus::resubscribe_all_locked: SUBSCRIBE {} failed: {}", ch, err.message);
#endif
                this->notify_error(err);
            }
        }

        for (auto& [pat, cb] : this->desired_patterns_)
        {
            auto r = co_await this->sub_client_->psubscribe(pat, cb);
            if (!r)
            {
                const auto& err = r.error();
#ifdef UREDIS_LOGS
                usub::ulog::info("RedisBus::resubscribe_all_locked: PSUBSCRIBE {} failed: {}", pat, err.message);
#endif
                this->notify_error(err);
            }
        }

        co_return RedisResult<void>{};
    }

    RedisResult<void> RedisBus::apply_subscribe_locked(
        const std::string& channel,
        const Callback& cb)
    {
        this->desired_channels_[channel] = cb;
        if (!this->connected_ || !this->sub_client_)
        {
            return RedisResult<void>{};
        }
        return RedisResult<void>{};
    }

    RedisResult<void> RedisBus::apply_psubscribe_locked(
        const std::string& pattern,
        const Callback& cb)
    {
        this->desired_patterns_[pattern] = cb;
        if (!this->connected_ || !this->sub_client_)
        {
            return RedisResult<void>{};
        }
        return RedisResult<void>{};
    }

    RedisResult<void> RedisBus::apply_unsubscribe_locked(const std::string& channel)
    {
        this->desired_channels_.erase(channel);
        return RedisResult<void>{};
    }

    RedisResult<void> RedisBus::apply_punsubscribe_locked(const std::string& pattern)
    {
        this->desired_patterns_.erase(pattern);
        return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void>> RedisBus::publish(
        std::string_view channel,
        std::string_view payload)
    {
        auto guard = co_await this->mutex_.lock();

        auto ec = co_await this->ensure_connected_locked();
        if (!ec)
            co_return std::unexpected(ec.error());

        if (!this->pub_client_)
        {
            RedisError err{RedisErrorCategory::Io, "RedisBus: pub_client is null"};
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        std::string ch{channel};
        std::string pl{payload};

        std::string_view args_arr[2] = {ch, pl};
        auto resp = co_await this->pub_client_->command(
            "PUBLISH",
            std::span<const std::string_view>(args_arr, 2));
        if (!resp)
        {
            auto err = resp.error();
#ifdef UREDIS_LOGS
            usub::ulog::error("RedisBus::publish: PUBLISH {} failed: {}", ch, err.message);
#endif
            this->connected_ = false;
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void>> RedisBus::subscribe(
        std::string channel,
        Callback cb)
    {
        auto guard = co_await this->mutex_.lock();

        this->desired_channels_[channel] = cb;

        auto ec = co_await this->ensure_connected_locked();
        if (!ec)
            co_return std::unexpected(ec.error());

        if (!this->sub_client_)
        {
            RedisError err{RedisErrorCategory::Io, "RedisBus: sub_client is null"};
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        auto r = co_await this->sub_client_->subscribe(channel, cb);
        if (!r)
        {
            auto err = r.error();
#ifdef UREDIS_LOGS
            usub::ulog::error("RedisBus::subscribe: SUBSCRIBE {} failed: {}", channel, err.message);
#endif
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void>> RedisBus::psubscribe(
        std::string pattern,
        Callback cb)
    {
        auto guard = co_await this->mutex_.lock();

        this->desired_patterns_[pattern] = cb;

        auto ec = co_await this->ensure_connected_locked();
        if (!ec)
            co_return std::unexpected(ec.error());

        if (!this->sub_client_)
        {
            RedisError err{RedisErrorCategory::Io, "RedisBus: sub_client is null"};
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        auto r = co_await this->sub_client_->psubscribe(pattern, cb);
        if (!r)
        {
            auto err = r.error();
#ifdef UREDIS_LOGS
            usub::ulog::error("RedisBus::psubscribe: PSUBSCRIBE {} failed: {}", pattern, err.message);
#endif
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void>> RedisBus::unsubscribe(std::string channel)
    {
        auto guard = co_await this->mutex_.lock();

        this->desired_channels_.erase(channel);

        if (!this->connected_ || !this->sub_client_)
        {
            co_return RedisResult<void>{};
        }

        auto r = co_await this->sub_client_->unsubscribe(channel);
        if (!r)
        {
            auto err = r.error();
#ifdef UREDIS_LOGS
            usub::ulog::error("RedisBus::unsubscribe: UNSUBSCRIBE {} failed: {}", channel, err.message);
#endif
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void>> RedisBus::punsubscribe(std::string pattern)
    {
        auto guard = co_await this->mutex_.lock();

        this->desired_patterns_.erase(pattern);

        if (!this->connected_ || !this->sub_client_)
        {
            co_return RedisResult<void>{};
        }

        auto r = co_await this->sub_client_->punsubscribe(pattern);
        if (!r)
        {
            auto err = r.error();
#ifdef UREDIS_LOGS
            usub::ulog::error("RedisBus::punsubscribe: PUNSUBSCRIBE {} failed: {}", pattern, err.message);
#endif
            this->notify_error(err);
            co_return std::unexpected(err);
        }

        co_return RedisResult<void>{};
    }

    task::Awaitable<void> RedisBus::run()
    {
        co_await this->run_loop();
        co_return;
    }

    task::Awaitable<void> RedisBus::run_loop()
    {
        using namespace std::chrono_literals;

        while (true)
        {
            {
                auto guard = co_await this->mutex_.lock();
                if (this->stopping_)
                    co_return;
            }

            {
                auto guard = co_await this->mutex_.lock();
                auto ec = co_await this->ensure_connected_locked();
                if (!ec)
                {
                    auto err = ec.error();
#ifdef UREDIS_LOGS
                    usub::ulog::info("RedisBus::run_loop: ensure_connected failed: {}", err.message);
#endif
                    this->connected_ = false;
                    this->notify_error(err);
                }
            }

            {
                auto guard = co_await this->mutex_.lock();

                if (this->stopping_)
                    co_return;

                if (!this->connected_
                    || !this->pub_client_
                    || !this->sub_client_
                    || !this->sub_client_->is_connected())
                {
#ifdef UREDIS_LOGS
                    usub::ulog::info("RedisBus::run_loop: not fully connected (sub?), sleep and retry");
#endif
                    this->connected_ = false;
                    co_await system::this_coroutine::sleep_for(
                        std::chrono::milliseconds(this->cfg_.reconnect_delay_ms));
                    continue;
                }

                std::string_view args_arr[0]{};
                auto resp = co_await this->pub_client_->command(
                    "PING",
                    std::span<const std::string_view>(args_arr, 0));
                if (!resp)
                {
                    auto err = resp.error();
#ifdef UREDIS_LOGS
                    usub::ulog::info("RedisBus::run_loop: PING failed: {}", err.message);
#endif
                    this->connected_ = false;
                    this->notify_error(err);
                    co_await system::this_coroutine::sleep_for(
                        std::chrono::milliseconds(this->cfg_.reconnect_delay_ms));
                    continue;
                }
            }

            co_await system::this_coroutine::sleep_for(
                std::chrono::milliseconds(this->cfg_.ping_interval_ms));
        }
    }

    task::Awaitable<void> RedisBus::close()
    {
        auto guard = co_await this->mutex_.lock();
        this->stopping_ = true;
        this->connected_ = false;

        if (this->sub_client_)
        {
            co_await this->sub_client_->close();
            this->sub_client_.reset();
        }

        if (this->pub_client_)
        {
            this->pub_client_->command("QUIT", std::span<const std::string_view>{});
            this->pub_client_.reset();
        }

        co_return;
    }
} // namespace usub::uredis
