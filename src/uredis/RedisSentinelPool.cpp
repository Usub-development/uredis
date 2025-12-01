#include "uredis/RedisSentinelPool.h"

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis
{
    namespace task = usub::uvent::task;

    RedisSentinelPool::RedisSentinelPool(RedisSentinelConfig cfg)
        : cfg_(std::move(cfg))
    {
        if (this->cfg_.pool_size == 0)
            this->cfg_.pool_size = 1;
    }

    task::Awaitable<RedisResult<void>> RedisSentinelPool::ensure_connected_locked()
    {
        if (this->connected_ && this->pool_)
        {
            co_return RedisResult<void>{};
        }

        auto master_res = co_await resolve_master_from_sentinel(this->cfg_);
        if (!master_res)
        {
            const auto& err = master_res.error();
#ifdef UREDIS_LOGS
            usub::ulog::error(
                "RedisSentinelPool::ensure_connected_locked: resolve_master failed: {}",
                err.message);
#endif
            co_return std::unexpected(err);
        }

        RedisConfig master_cfg = master_res.value();

        RedisPoolConfig pcfg;
        pcfg.host = master_cfg.host;
        pcfg.port = master_cfg.port;
        pcfg.db   = master_cfg.db;
        pcfg.username = master_cfg.username;
        pcfg.password = master_cfg.password;
        pcfg.connect_timeout_ms = master_cfg.connect_timeout_ms;
        pcfg.io_timeout_ms      = master_cfg.io_timeout_ms;
        pcfg.size               = this->cfg_.pool_size;

        auto pool = std::make_shared<RedisPool>(pcfg);
        auto rc = co_await pool->connect_all();
        if (!rc)
        {
            const auto& err = rc.error();
#ifdef UREDIS_LOGS
            usub::ulog::error(
                "RedisSentinelPool::ensure_connected_locked: connect_all failed: {}",
                err.message);
#endif
            co_return std::unexpected(err);
        }

        this->pool_      = std::move(pool);
        this->connected_ = true;

#ifdef UREDIS_LOGS
        usub::ulog::info(
            "RedisSentinelPool: connected to master {}:{} (db={})",
            master_cfg.host,
            master_cfg.port,
            master_cfg.db);
#endif

        co_return RedisResult<void>{};
    }

    task::Awaitable<RedisResult<void>> RedisSentinelPool::connect()
    {
        auto guard = co_await this->mutex_.lock();
        co_return co_await this->ensure_connected_locked();
    }

    task::Awaitable<RedisResult<RedisValue>> RedisSentinelPool::command(
        std::string_view cmd,
        std::span<const std::string_view> args)
    {
        std::shared_ptr<RedisPool> pool;

        {
            auto guard = co_await this->mutex_.lock();
            auto ec = co_await this->ensure_connected_locked();
            if (!ec)
                co_return std::unexpected(ec.error());

            pool = this->pool_;
        }

        auto resp = co_await pool->command(cmd, args);
        if (resp)
            co_return resp;

        auto err = resp.error();

        if (err.category != RedisErrorCategory::Io)
        {
#ifdef UREDIS_LOGS
            usub::ulog::error(
                "RedisSentinelPool::command: command '{}' failed (no retry), error={}",
                cmd,
                err.message);
#endif
            co_return std::unexpected(err);
        }

#ifdef UREDIS_LOGS
        usub::ulog::warn(
            "RedisSentinelPool::command: Io error on command '{}', will re-resolve master and retry once: {}",
            cmd,
            err.message);
#endif

        {
            auto guard = co_await this->mutex_.lock();
            this->connected_ = false;
            this->pool_.reset();

            auto ec = co_await this->ensure_connected_locked();
            if (!ec)
            {
#ifdef UREDIS_LOGS
                const auto& e2 = ec.error();
                usub::ulog::error(
                    "RedisSentinelPool::command: reconnection failed, keep old error: {}",
                    e2.message);
#endif
                co_return std::unexpected(err);
            }

            pool = this->pool_;
        }

        auto resp2 = co_await pool->command(cmd, args);
        co_return resp2;
    }
} // namespace usub::uredis