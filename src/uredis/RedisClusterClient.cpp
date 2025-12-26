#include "uredis/RedisClusterClient.h"

#include <charconv>
#include <chrono>

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis {
    using namespace std::chrono_literals;

    static bool is_cluster_disabled_error(const RedisError &e) {
        if (e.category != RedisErrorCategory::ServerReply)
            return false;
        return e.message.find("cluster support disabled") != std::string::npos;
    }

    RedisClusterClient::RedisClusterClient(RedisClusterConfig cfg)
        : cfg_(std::move(cfg)) {
        this->slot_to_node_.fill(-1);
        if (this->cfg_.max_redirections <= 0)
            this->cfg_.max_redirections = 5;

        if (this->cfg_.max_connections_per_node == 0)
            this->cfg_.max_connections_per_node = 1;
    }

    std::string_view RedisClusterClient::extract_hash_tag(std::string_view key) {
        auto l = key.find('{');
        if (l == std::string_view::npos) return key;
        auto r = key.find('}', l + 1);
        if (r == std::string_view::npos || r == l + 1) return key;
        return key.substr(l + 1, r - l - 1);
    }

    std::uint16_t RedisClusterClient::calc_slot(std::string_view key) {
        if (key.empty()) return 0;

        std::uint16_t crc = 0;
        for (unsigned char b: key) {
            crc ^= static_cast<std::uint16_t>(b) << 8;
            for (int i = 0; i < 8; ++i) {
                if (crc & 0x8000)
                    crc = static_cast<std::uint16_t>((crc << 1) ^ 0x1021);
                else
                    crc = static_cast<std::uint16_t>(crc << 1);
            }
        }
        return crc % 16384;
    }

    std::optional<RedisClusterClient::Redirection>
    RedisClusterClient::parse_redirection(const std::string &msg) {
        std::string_view s{msg};

        auto next_token = [](std::string_view &str) -> std::string_view {
            while (!str.empty() && str.front() == ' ')
                str.remove_prefix(1);
            if (str.empty()) return {};
            std::size_t pos = str.find(' ');
            if (pos == std::string_view::npos) {
                std::string_view t = str;
                str = {};
                return t;
            }
            std::string_view t = str.substr(0, pos);
            str.remove_prefix(pos + 1);
            return t;
        };

        auto t0 = next_token(s);
        if (t0.empty()) return std::nullopt;

        RedirType type;
        if (t0 == "MOVED")
            type = RedirType::Moved;
        else if (t0 == "ASK")
            type = RedirType::Ask;
        else
            return std::nullopt;

        auto t1 = next_token(s);
        auto t2 = next_token(s);
        if (t1.empty() || t2.empty()) return std::nullopt;

        int slot = 0;
        auto [p1, ec1] = std::from_chars(t1.data(), t1.data() + t1.size(), slot);
        (void) p1;
        if (ec1 != std::errc{}) return std::nullopt;

        std::size_t colon = t2.find(':');
        if (colon == std::string_view::npos) return std::nullopt;

        std::string host{t2.substr(0, colon)};
        int port_i = 0;
        auto [p2, ec2] = std::from_chars(
            t2.data() + colon + 1,
            t2.data() + t2.size(),
            port_i);
        (void) p2;
        if (ec2 != std::errc{}) return std::nullopt;
        if (port_i <= 0 || port_i > 65535) return std::nullopt;

        Redirection r;
        r.type = type;
        r.slot = slot;
        r.host = std::move(host);
        r.port = static_cast<std::uint16_t>(port_i);
        return r;
    }

    RedisResult<int> RedisClusterClient::node_index_for_slot_nolock(int slot) {
        if (slot < 0 || slot >= 16384) {
            RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: invalid slot"};
            return std::unexpected(err);
        }

        int idx = this->slot_to_node_[static_cast<std::size_t>(slot)];
        if (idx < 0 || static_cast<std::size_t>(idx) >= this->nodes_.size()) {
            RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: slot mapping is empty"};
            return std::unexpected(err);
        }

        return idx;
    }

    RedisResult<int> RedisClusterClient::node_index_for_key_nolock(std::string_view key) {
        if (this->nodes_.empty()) {
            RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: no nodes"};
            return std::unexpected(err);
        }

        if (key.empty())
            return 0;

        std::string key_copy{key};
        std::string_view key_view{key_copy};
        auto tag = extract_hash_tag(key_view);
        auto slot = calc_slot(tag.empty() ? key_view : tag);

        return node_index_for_slot_nolock(static_cast<int>(slot));
    }

    task::Awaitable<RedisResult<void> > RedisClusterClient::initial_discovery() {
        if (this->cfg_.seeds.empty()) {
            RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: seeds list is empty"};
            co_return std::unexpected(err);
        }

        for (const auto &seed: this->cfg_.seeds) {
            auto mc = co_await this->get_or_create_main_client_for_node(seed.host, seed.port);
            if (!mc) {
#ifdef UREDIS_LOGS
                const auto &e = mc.error();
                usub::ulog::warn(
                    "RedisClusterClient::initial_discovery: seed {}:{} connect failed: {}",
                    seed.host, seed.port, e.message);
#endif
                continue;
            }

            auto client = mc.value();

            std::string_view arg = "SLOTS";
            std::array<std::string_view, 1> args_arr{arg};
            auto resp = co_await client->command(
                "CLUSTER",
                std::span<const std::string_view>(args_arr.data(), args_arr.size()));

            if (!resp) {
                const auto &e = resp.error();

#ifdef UREDIS_LOGS
                usub::ulog::warn(
                    "RedisClusterClient::initial_discovery: CLUSTER SLOTS on {}:{} failed: {}",
                    seed.host, seed.port, e.message);
#endif

                if (is_cluster_disabled_error(e)) {
                    {
                        auto guard = co_await this->mutex_.lock();

                        if (this->nodes_.empty()) {
                            for (const auto &s: this->cfg_.seeds) {
                                RedisConfig ncfg;
                                ncfg.host = s.host;
                                ncfg.port = s.port;
                                ncfg.db = 0;
                                ncfg.username = this->cfg_.username;
                                ncfg.password = this->cfg_.password;
                                ncfg.connect_timeout_ms = this->cfg_.connect_timeout_ms;
                                ncfg.io_timeout_ms = this->cfg_.io_timeout_ms;

                                auto node = std::make_shared<Node>(ncfg, this->cfg_.max_connections_per_node);
                                this->nodes_.push_back(node);
                            }
                        }

                        this->slot_to_node_.fill(0);
                        this->standalone_mode_ = true;
                    }

#ifdef UREDIS_LOGS
                    usub::ulog::info(
                        "RedisClusterClient::initial_discovery: cluster disabled on {}:{}, fallback to standalone pool mode",
                        seed.host, seed.port);
#endif

                    for (const auto &node: this->nodes_) {
                        for (std::size_t i = 0; i < this->cfg_.max_connections_per_node; ++i) {
                            auto cli = std::make_shared<RedisClient>(node->cfg);
                            auto c = co_await cli->connect();
                            if (!c)
                                break;

                            if (!node->idle.try_enqueue(cli))
                                break;

                            node->live_count.fetch_add(1, std::memory_order_relaxed);
                            node->idle_sem.release();
                        }
                    }

                    co_return RedisResult<void>{};
                }

                continue;
            }

            const RedisValue &v = *resp;
            if (!v.is_array()) {
#ifdef UREDIS_LOGS
                usub::ulog::warn(
                    "RedisClusterClient::initial_discovery: CLUSTER SLOTS reply not array from {}:{}",
                    seed.host, seed.port);
#endif
                continue;
            }

            const auto &slot_ranges = v.as_array(); {
                auto guard = co_await this->mutex_.lock();

                this->slot_to_node_.fill(-1);

                auto ensure_node_locked = [this, &seed](const RedisValue &node_val) -> std::optional<int> {
                    if (!node_val.is_array())
                        return std::nullopt;

                    const auto &arr = node_val.as_array();
                    if (arr.size() < 2)
                        return std::nullopt;

                    if (!arr[0].is_bulk_string() && !arr[0].is_simple_string())
                        return std::nullopt;
                    if (!arr[1].is_integer())
                        return std::nullopt;

                    std::string host = arr[0].as_string();
                    int64_t port_i = arr[1].as_integer();
                    if (port_i <= 0 || port_i > 65535)
                        return std::nullopt;

                    if (host.empty())
                        host = seed.host;

                    std::uint16_t port = static_cast<std::uint16_t>(port_i);

                    for (std::size_t i = 0; i < this->nodes_.size(); ++i) {
                        if (this->nodes_[i]->cfg.host == host &&
                            this->nodes_[i]->cfg.port == port) {
                            return static_cast<int>(i);
                        }
                    }

                    RedisConfig ncfg;
                    ncfg.host = std::move(host);
                    ncfg.port = port;
                    ncfg.db = 0;
                    ncfg.username = this->cfg_.username;
                    ncfg.password = this->cfg_.password;
                    ncfg.connect_timeout_ms = this->cfg_.connect_timeout_ms;
                    ncfg.io_timeout_ms = this->cfg_.io_timeout_ms;

                    auto node = std::make_shared<Node>(ncfg, this->cfg_.max_connections_per_node);
                    this->nodes_.push_back(node);
                    return static_cast<int>(this->nodes_.size() - 1);
                };

                for (const auto &range_val: slot_ranges) {
                    if (!range_val.is_array()) continue;
                    const auto &range_arr = range_val.as_array();
                    if (range_arr.size() < 3) continue;

                    if (!range_arr[0].is_integer() || !range_arr[1].is_integer()) continue;

                    int64_t start = range_arr[0].as_integer();
                    int64_t end = range_arr[1].as_integer();

                    auto master_idx_opt = ensure_node_locked(range_arr[2]);
                    if (!master_idx_opt) continue;
                    int master_idx = *master_idx_opt;

                    if (start < 0) start = 0;
                    if (end > 16383) end = 16383;

                    for (int64_t s = start; s <= end; ++s)
                        this->slot_to_node_[static_cast<std::size_t>(s)] = master_idx;

                    for (std::size_t i = 3; i < range_arr.size(); ++i)
                        (void) ensure_node_locked(range_arr[i]);
                }
            }

#ifdef UREDIS_LOGS
            usub::ulog::info(
                "RedisClusterClient::initial_discovery: CLUSTER SLOTS ok via \"{}\":{}",
                seed.host, seed.port);
#endif

            for (const auto &node: this->nodes_) {
                for (std::size_t i = 0; i < this->cfg_.max_connections_per_node; ++i) {
                    auto cli = std::make_shared<RedisClient>(node->cfg);
                    auto c = co_await cli->connect();
                    if (!c)
                        break;

                    if (!node->idle.try_enqueue(cli))
                        break;

                    node->live_count.fetch_add(1, std::memory_order_relaxed);
                    node->idle_sem.release();
                }
            }

            co_return RedisResult<void>{};
        }

        RedisError err{RedisErrorCategory::Io, "RedisClusterClient: CLUSTER SLOTS failed on all seeds"};
        co_return std::unexpected(err);
    }

    task::Awaitable<RedisResult<void> > RedisClusterClient::connect() {
        if (this->init_finished_)
            co_return *this->init_result_;

        bool we_are_initializer = false; {
            auto guard = co_await this->init_mutex_.lock();

            if (this->init_finished_)
                co_return *this->init_result_;

            if (!this->init_started_) {
                this->init_started_ = true;
                we_are_initializer = true;
            }
        }

        if (!we_are_initializer) {
            co_await this->init_event_.wait();
            co_return *this->init_result_;
        }

        auto res = co_await this->initial_discovery(); {
            auto guard = co_await this->init_mutex_.lock();
            this->init_result_ = res;
            this->init_finished_ = true;
            this->init_event_.set();
        }

        co_return res;
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
    RedisClusterClient::get_or_create_main_client_for_node(
        std::string_view host,
        std::uint16_t port) {
        std::shared_ptr<Node> node; {
            auto guard = co_await this->mutex_.lock();

            for (auto &n: this->nodes_) {
                if (n->cfg.host == host && n->cfg.port == port) {
                    node = n;
                    break;
                }
            }

            if (!node) {
                RedisConfig cfg;
                cfg.host = std::string(host);
                cfg.port = port;
                cfg.db = 0;
                cfg.username = this->cfg_.username;
                cfg.password = this->cfg_.password;
                cfg.connect_timeout_ms = this->cfg_.connect_timeout_ms;
                cfg.io_timeout_ms = this->cfg_.io_timeout_ms;

                node = std::make_shared<Node>(cfg, this->cfg_.max_connections_per_node);
                this->nodes_.push_back(node);
            }
        }

        if (node->main_client && node->main_client->connected())
            co_return node->main_client;

        auto cli = std::make_shared<RedisClient>(node->cfg);
        auto c = co_await cli->connect();
        if (!c)
            co_return std::unexpected(c.error()); {
            auto guard = co_await this->mutex_.lock();
            node->main_client = cli;
        }

        co_return cli;
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_client_for_node_locked(
        const std::shared_ptr<Node> &node) {
        std::shared_ptr<RedisClient> client;

        for (;;) {
            if (node->idle.try_dequeue(client)) {
                if (!client)
                    continue;

                if (!client->connected()) {
                    node->live_count.fetch_sub(1, std::memory_order_relaxed);
                    node->notify_waiters_if_any();
                    continue;
                }

                if (!client->is_idle()) {
                    node->live_count.fetch_sub(1, std::memory_order_relaxed);
                    node->notify_waiters_if_any();
                    continue;
                }

                co_return PooledClient{node, std::move(client)};
            }

            auto cur_live = node->live_count.load(std::memory_order_relaxed);
            if (cur_live < this->cfg_.max_connections_per_node) {
                if (node->live_count.compare_exchange_strong(
                    cur_live,
                    cur_live + 1,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                    auto cli = std::make_shared<RedisClient>(node->cfg);
                    auto c = co_await cli->connect();
                    if (!c) {
                        node->live_count.fetch_sub(1, std::memory_order_relaxed);
                        node->notify_waiters_if_any();
                        co_return std::unexpected(c.error());
                    }

                    co_return PooledClient{node, std::move(cli)};
                }

                continue;
            }

            node->waiters.fetch_add(1, std::memory_order_relaxed);
            co_await node->idle_sem.acquire();
            node->waiters.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    task::Awaitable<void>
    RedisClusterClient::release_pooled_client(PooledClient &&pc, bool connection_faulty) {
        auto node = std::move(pc.node);
        auto client = std::move(pc.client);

        if (!node || !client)
            co_return;

        if (connection_faulty || !client->connected()) {
            node->live_count.fetch_sub(1, std::memory_order_relaxed);
            node->notify_waiters_if_any();
            co_return;
        }

        if (!client->is_idle()) {
            node->live_count.fetch_sub(1, std::memory_order_relaxed);
            node->notify_waiters_if_any();
            co_return;
        }

        if (!node->idle.try_enqueue(client)) {
            node->live_count.fetch_sub(1, std::memory_order_relaxed);
            node->notify_waiters_if_any();
            co_return;
        }

        node->idle_sem.release();
        co_return;
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_client_for_slot(int slot) {
        auto init = co_await this->connect();
        if (!init)
            co_return std::unexpected(init.error());

        if (slot < 0 || slot >= 16384) {
            RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: invalid slot"};
            co_return std::unexpected(err);
        }

        std::shared_ptr<Node> node; {
            auto guard = co_await this->mutex_.lock();
            auto idx_res = this->node_index_for_slot_nolock(slot);
            if (!idx_res)
                co_return std::unexpected(idx_res.error());

            int idx = idx_res.value();
            if (idx < 0 || static_cast<std::size_t>(idx) >= this->nodes_.size()) {
                RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: node index out of range"};
                co_return std::unexpected(err);
            }

            node = this->nodes_[static_cast<std::size_t>(idx)];
        }

        co_return co_await this->acquire_client_for_node_locked(node);
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_client_for_any() {
        auto init = co_await this->connect();
        if (!init)
            co_return std::unexpected(init.error());

        std::shared_ptr<Node> node; {
            auto guard = co_await this->mutex_.lock();
            if (this->nodes_.empty()) {
                RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: no nodes"};
                co_return std::unexpected(err);
            }
            node = this->nodes_.front();
        }

        co_return co_await this->acquire_client_for_node_locked(node);
    }

    task::Awaitable<RedisResult<RedisClusterClient::PooledClient> >
    RedisClusterClient::acquire_client_for_key(std::string_view key) {
        auto init = co_await this->connect();
        if (!init)
            co_return std::unexpected(init.error());

        if (key.empty())
            co_return co_await this->acquire_client_for_any();

        std::shared_ptr<Node> node; {
            auto guard = co_await this->mutex_.lock();
            auto idx_res = this->node_index_for_key_nolock(key);
            if (!idx_res)
                co_return std::unexpected(idx_res.error());

            int idx = idx_res.value();
            if (idx < 0 || static_cast<std::size_t>(idx) >= this->nodes_.size()) {
                RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: node index out of range"};
                co_return std::unexpected(err);
            }

            node = this->nodes_[static_cast<std::size_t>(idx)];
        }

        co_return co_await this->acquire_client_for_node_locked(node);
    }

    task::Awaitable<void> RedisClusterClient::apply_moved(const Redirection &r) {
        if (r.slot < 0 || r.slot >= 16384)
            co_return;

        auto mc = co_await this->get_or_create_main_client_for_node(r.host, r.port);
        if (!mc)
            co_return;

        auto guard = co_await this->mutex_.lock();

        int idx = -1;
        for (std::size_t i = 0; i < this->nodes_.size(); ++i) {
            if (this->nodes_[i]->cfg.host == r.host &&
                this->nodes_[i]->cfg.port == r.port) {
                idx = static_cast<int>(i);
                break;
            }
        }

        if (idx >= 0)
            this->slot_to_node_[static_cast<std::size_t>(r.slot)] = idx;

        co_return;
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
    RedisClusterClient::get_client_for_key(std::string_view key) {
        auto init = co_await this->connect();
        if (!init)
            co_return std::unexpected(init.error());

        if (key.empty())
            co_return co_await this->get_any_client();

        std::string host;
        std::uint16_t port{0}; {
            auto guard = co_await this->mutex_.lock();
            auto idx_res = this->node_index_for_key_nolock(key);
            if (!idx_res)
                co_return std::unexpected(idx_res.error());

            int idx = idx_res.value();
            host = this->nodes_[static_cast<std::size_t>(idx)]->cfg.host;
            port = this->nodes_[static_cast<std::size_t>(idx)]->cfg.port;
        }

        co_return co_await this->get_or_create_main_client_for_node(host, port);
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
    RedisClusterClient::get_any_client() {
        auto init = co_await this->connect();
        if (!init)
            co_return std::unexpected(init.error());

        std::string host;
        std::uint16_t port{0}; {
            auto guard = co_await this->mutex_.lock();
            if (this->nodes_.empty()) {
                RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: no nodes"};
                co_return std::unexpected(err);
            }

            host = this->nodes_.front()->cfg.host;
            port = this->nodes_.front()->cfg.port;
        }

        co_return co_await this->get_or_create_main_client_for_node(host, port);
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
    RedisClusterClient::get_client_for_slot(int slot) {
        auto init = co_await this->connect();
        if (!init)
            co_return std::unexpected(init.error());

        std::string host;
        std::uint16_t port{0}; {
            auto guard = co_await this->mutex_.lock();
            auto idx_res = this->node_index_for_slot_nolock(slot);
            if (!idx_res)
                co_return std::unexpected(idx_res.error());

            int idx = idx_res.value();
            host = this->nodes_[static_cast<std::size_t>(idx)]->cfg.host;
            port = this->nodes_[static_cast<std::size_t>(idx)]->cfg.port;
        }

        co_return co_await this->get_or_create_main_client_for_node(host, port);
    }

    task::Awaitable<RedisResult<RedisValue> >
    RedisClusterClient::command(
        std::string_view cmd,
        std::span<const std::string_view> args) {
        auto init = co_await this->connect();
        if (!init)
            co_return std::unexpected(init.error());

        std::string key_copy;
        if (!args.empty())
            key_copy.assign(args[0].begin(), args[0].end());

        for (int attempt = 0; attempt < this->cfg_.max_redirections; ++attempt) {
            PooledClient pc;

            if (args.empty()) {
                auto ac = co_await this->acquire_client_for_any();
                if (!ac) co_return std::unexpected(ac.error());
                pc = ac.value();
            } else {
                auto ac = co_await this->acquire_client_for_key(key_copy);
                if (!ac) co_return std::unexpected(ac.error());
                pc = ac.value();
            }

            auto resp = co_await pc.client->command(cmd, args);
            if (resp) {
                co_await this->release_pooled_client(std::move(pc), false);
                co_return resp;
            }

            auto err = resp.error();

            if (err.category != RedisErrorCategory::ServerReply) {
                co_await this->release_pooled_client(std::move(pc), true);
                co_return std::unexpected(err);
            }

            co_await this->release_pooled_client(std::move(pc), false);

            auto redir_opt = parse_redirection(err.message);
            if (!redir_opt)
                co_return std::unexpected(err);

            const auto &redir = *redir_opt;

            if (redir.type == RedirType::Moved) {
                co_await this->apply_moved(redir);
                continue;
            }

            if (redir.type == RedirType::Ask) {
                auto mc = co_await this->get_or_create_main_client_for_node(redir.host, redir.port);
                if (!mc) co_return std::unexpected(mc.error());
                auto ask_client = mc.value();

                std::span<const std::string_view> no_args;
                (void) co_await ask_client->command("ASKING", no_args);

                auto resp2 = co_await ask_client->command(cmd, args);
                if (resp2)
                    co_return resp2;

                auto err2 = resp2.error();
                auto redir2 = parse_redirection(err2.message);
                if (!redir2)
                    co_return std::unexpected(err2);

                if (redir2->type == RedirType::Moved) {
                    co_await this->apply_moved(*redir2);
                    continue;
                }

                co_return std::unexpected(err2);
            }

            co_return std::unexpected(err);
        }

        RedisError err{RedisErrorCategory::Protocol, "RedisClusterClient: too many redirections"};
        co_return std::unexpected(err);
    }
} // namespace usub::uredis
