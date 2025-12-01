#include "uredis/RedisClusterClient.h"

#include <charconv>

#ifdef UREDIS_LOGS
#include <ulog/ulog.h>
#endif

namespace usub::uredis
{
    using usub::uvent::sync::AsyncMutex;

    RedisClusterClient::RedisClusterClient(RedisClusterConfig cfg)
        : cfg_(std::move(cfg))
    {
        this->slot_to_node_.fill(-1);
        if (this->cfg_.max_redirections <= 0)
        {
            this->cfg_.max_redirections = 5;
        }
    }

    std::string_view RedisClusterClient::extract_hash_tag(std::string_view key)
    {
        auto l = key.find('{');
        if (l == std::string_view::npos) return key;
        auto r = key.find('}', l + 1);
        if (r == std::string_view::npos || r == l + 1) return key;
        return key.substr(l + 1, r - l - 1);
    }

    std::uint16_t RedisClusterClient::calc_slot(std::string_view key)
    {
        if (key.empty()) return 0;

        std::uint16_t crc = 0;
        for (unsigned char b : key)
        {
            crc ^= static_cast<std::uint16_t>(b) << 8;
            for (int i = 0; i < 8; ++i)
            {
                if (crc & 0x8000)
                    crc = static_cast<std::uint16_t>((crc << 1) ^ 0x1021);
                else
                    crc = static_cast<std::uint16_t>(crc << 1);
            }
        }
        return crc % 16384;
    }

    std::optional<RedisClusterClient::Redirection>
    RedisClusterClient::parse_redirection(const std::string& msg)
    {
        std::string_view s{msg};

        auto next_token = [](std::string_view& str) -> std::string_view
        {
            while (!str.empty() && str.front() == ' ')
                str.remove_prefix(1);
            if (str.empty()) return {};
            std::size_t pos = str.find(' ');
            if (pos == std::string_view::npos)
            {
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
        if (ec1 != std::errc{}) return std::nullopt;

        std::size_t colon = t2.find(':');
        if (colon == std::string_view::npos) return std::nullopt;

        std::string host{t2.substr(0, colon)};
        int port_i = 0;
        auto [p2, ec2] = std::from_chars(
            t2.data() + colon + 1,
            t2.data() + t2.size(),
            port_i);
        if (ec2 != std::errc{}) return std::nullopt;
        if (port_i <= 0 || port_i > 65535) return std::nullopt;

        Redirection r;
        r.type = type;
        r.slot = slot;
        r.host = std::move(host);
        r.port = static_cast<std::uint16_t>(port_i);
        return r;
    }

    task::Awaitable<RedisResult<void>> RedisClusterClient::initial_discovery()
    {
        if (this->cfg_.seeds.empty())
        {
            RedisError err{
                RedisErrorCategory::Protocol,
                "RedisClusterClient: seeds list is empty"
            };
            co_return std::unexpected(err);
        }

        for (const auto& seed : this->cfg_.seeds)
        {
            RedisConfig rc;
            rc.host = seed.host;
            rc.port = seed.port;
            rc.db = 0;
            rc.username = this->cfg_.username;
            rc.password = this->cfg_.password;
            rc.connect_timeout_ms = this->cfg_.connect_timeout_ms;
            rc.io_timeout_ms = this->cfg_.io_timeout_ms;

            RedisClient cli{rc};
            auto c = co_await cli.connect();
            if (!c)
            {
#ifdef UREDIS_LOGS
                auto& e = c.error();
                usub::ulog::warn("RedisClusterClient::initial_discovery: seed {}:{} connect failed: {}",
                                 seed.host,
                                 seed.port,
                                 e.message);
#endif
                continue;
            }

            std::string_view arg = "SLOTS";
            std::string_view args_arr[1] = {arg};
            auto resp = co_await cli.command(
                "CLUSTER",
                std::span<const std::string_view>(args_arr, 1));
            if (!resp)
            {
#ifdef UREDIS_LOGS
                auto& e = resp.error();
                usub::ulog::warn("RedisClusterClient::initial_discovery: CLUSTER SLOTS on {}:{} failed: {}",
                                 seed.host,
                                 seed.port,
                                 e.message);
#endif
                continue;
            }

            const RedisValue& v = *resp;
            if (v.type != RedisType::Array)
            {
#ifdef UREDIS_LOGS
                usub::ulog::warn("RedisClusterClient::initial_discovery: CLUSTER SLOTS reply not array");
#endif
                continue;
            }

            {
                auto guard = co_await this->mutex_.lock();

                this->nodes_.clear();
                this->slot_to_node_.fill(-1);

                const auto& slot_ranges = v.as_array();
                for (const auto& range_val : slot_ranges)
                {
                    if (!range_val.is_array()) continue;
                    const auto& range_arr = range_val.as_array();
                    if (range_arr.size() < 3) continue;

                    if (!range_arr[0].is_integer() || !range_arr[1].is_integer()) continue;

                    int64_t start = range_arr[0].as_integer();
                    int64_t end = range_arr[1].as_integer();

                    const auto& master_val = range_arr[2];
                    if (!master_val.is_array()) continue;
                    const auto& master_arr = master_val.as_array();
                    if (master_arr.size() < 2) continue;

                    if (!master_arr[0].is_bulk_string() && !master_arr[0].is_simple_string())
                        continue;
                    if (!master_arr[1].is_integer())
                        continue;

                    std::string host = master_arr[0].as_string();
                    int64_t port_i = master_arr[1].as_integer();
                    if (port_i <= 0 || port_i > 65535) continue;
                    std::uint16_t port = static_cast<std::uint16_t>(port_i);

                    int node_index = -1;
                    for (std::size_t i = 0; i < this->nodes_.size(); ++i)
                    {
                        if (this->nodes_[i].cfg.host == host &&
                            this->nodes_[i].cfg.port == port)
                        {
                            node_index = static_cast<int>(i);
                            break;
                        }
                    }

                    if (node_index < 0)
                    {
                        RedisConfig ncfg;
                        ncfg.host = host;
                        ncfg.port = port;
                        ncfg.db = 0;
                        ncfg.username = this->cfg_.username;
                        ncfg.password = this->cfg_.password;
                        ncfg.connect_timeout_ms = this->cfg_.connect_timeout_ms;
                        ncfg.io_timeout_ms = this->cfg_.io_timeout_ms;

                        this->nodes_.push_back(Node{ncfg, nullptr});
                        node_index = static_cast<int>(this->nodes_.size() - 1);
                    }

                    if (start < 0) start = 0;
                    if (end > 16383) end = 16383;
                    for (int64_t s = start; s <= end; ++s)
                    {
                        this->slot_to_node_[static_cast<std::size_t>(s)] = node_index;
                    }
                }
            }

#ifdef UREDIS_LOGS
            usub::ulog::info("RedisClusterClient::initial_discovery: CLUSTER SLOTS ok via {}:{}",
                             seed.host,
                             seed.port);
#endif

            co_return RedisResult<void>{};
        }

        RedisError err{
            RedisErrorCategory::Io,
            "RedisClusterClient: CLUSTER SLOTS failed on all seeds"
        };
        co_return std::unexpected(err);
    }

    task::Awaitable<RedisResult<void>> RedisClusterClient::connect()
    {
        co_return co_await this->initial_discovery();
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
    RedisClusterClient::get_or_create_client_for_node(
        std::string_view host,
        std::uint16_t port)
    {
        auto guard = co_await this->mutex_.lock();

        Node* node = nullptr;
        for (auto& n : this->nodes_)
        {
            if (n.cfg.host == host && n.cfg.port == port)
            {
                node = &n;
                break;
            }
        }

        if (!node)
        {
            RedisConfig rc;
            rc.host = std::string(host);
            rc.port = port;
            rc.db = 0;
            rc.username = this->cfg_.username;
            rc.password = this->cfg_.password;
            rc.connect_timeout_ms = this->cfg_.connect_timeout_ms;
            rc.io_timeout_ms = this->cfg_.io_timeout_ms;

            this->nodes_.push_back(Node{rc, nullptr});
            node = &this->nodes_.back();
        }

        if (!node->client)
        {
            node->client = std::make_shared<RedisClient>(node->cfg);
            auto c = co_await node->client->connect();
            if (!c)
            {
                auto err = c.error();
                node->client.reset();
                co_return std::unexpected(err);
            }
        }

        co_return node->client;
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
    RedisClusterClient::get_or_create_client_for_slot_internal(
        int slot)
    {
        if (slot < 0 || slot >= 16384)
        {
            RedisError err{
                RedisErrorCategory::Protocol,
                "RedisClusterClient: invalid slot"
            };
            co_return std::unexpected(err);
        }

        std::string host;
        std::uint16_t port{0};

        {
            auto guard = co_await this->mutex_.lock();

            if (this->nodes_.empty())
            {
                RedisError err{
                    RedisErrorCategory::Protocol,
                    "RedisClusterClient: no nodes for slot"
                };
                co_return std::unexpected(err);
            }

            int idx = this->slot_to_node_[slot];
            if (idx < 0 || static_cast<std::size_t>(idx) >= this->nodes_.size())
            {
                RedisError err{
                    RedisErrorCategory::Protocol,
                    "RedisClusterClient: slot mapping is empty"
                };
                co_return std::unexpected(err);
            }

            host = this->nodes_[idx].cfg.host;
            port = this->nodes_[idx].cfg.port;
        }

        co_return co_await this->get_or_create_client_for_node(host, port);
    }

    task::Awaitable<void> RedisClusterClient::apply_moved(const Redirection& r)
    {
        if (r.slot < 0 || r.slot >= 16384)
            co_return;

        auto c = co_await this->get_or_create_client_for_node(r.host, r.port);
        if (!c)
            co_return;

        auto guard = co_await this->mutex_.lock();

        int idx = -1;
        for (std::size_t i = 0; i < this->nodes_.size(); ++i)
        {
            if (this->nodes_[i].cfg.host == r.host &&
                this->nodes_[i].cfg.port == r.port)
            {
                idx = static_cast<int>(i);
                break;
            }
        }

        if (idx >= 0)
        {
            this->slot_to_node_[static_cast<std::size_t>(r.slot)] = idx;
        }

        co_return;
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
    RedisClusterClient::get_client_for_key(std::string_view key)
    {
        if (this->nodes_.empty())
        {
            auto d = co_await this->initial_discovery();
            if (!d)
                co_return std::unexpected(d.error());
        }

        if (key.empty())
        {
            co_return co_await this->get_any_client();
        }

        std::string key_copy{key};
        std::string_view key_view{key_copy};
        auto tag  = extract_hash_tag(key_view);
        auto slot = calc_slot(tag.empty() ? key_view : tag);

        co_return co_await this->get_or_create_client_for_slot_internal(static_cast<int>(slot));
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
    RedisClusterClient::get_any_client()
    {
        if (this->nodes_.empty())
        {
            auto d = co_await this->initial_discovery();
            if (!d)
                co_return std::unexpected(d.error());
        }

        std::string host;
        std::uint16_t port{0};

        {
            auto guard = co_await this->mutex_.lock();
            if (this->nodes_.empty())
            {
                RedisError err{
                    RedisErrorCategory::Protocol,
                    "RedisClusterClient: no nodes for get_any_client"
                };
                co_return std::unexpected(err);
            }
            host = this->nodes_.front().cfg.host;
            port = this->nodes_.front().cfg.port;
        }

        co_return co_await this->get_or_create_client_for_node(host, port);
    }

    task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>>
    RedisClusterClient::get_client_for_slot(int slot)
    {
        if (this->nodes_.empty())
        {
            auto d = co_await this->initial_discovery();
            if (!d)
                co_return std::unexpected(d.error());
        }

        co_return co_await this->get_or_create_client_for_slot_internal(slot);
    }

    task::Awaitable<RedisResult<RedisValue>> RedisClusterClient::command(
        std::string_view cmd,
        std::span<const std::string_view> args)
    {
        if (this->nodes_.empty())
        {
            auto d = co_await this->initial_discovery();
            if (!d)
                co_return std::unexpected(d.error());
        }

        std::string key_copy;
        if (!args.empty())
        {
            key_copy.assign(args[0].begin(), args[0].end());
        }

        for (int attempt = 0; attempt < this->cfg_.max_redirections; ++attempt)
        {
            std::shared_ptr<RedisClient> client;

            if (args.empty())
            {
                auto c = co_await this->get_any_client();
                if (!c)
                    co_return std::unexpected(c.error());
                client = c.value();
            }
            else
            {
                auto c = co_await this->get_client_for_key(key_copy);
                if (!c)
                    co_return std::unexpected(c.error());
                client = c.value();
            }

            auto resp = co_await client->command(cmd, args);
            if (resp)
            {
                co_return resp;
            }

            auto err = resp.error();

            if (err.category != RedisErrorCategory::ServerReply)
            {
                co_return std::unexpected(err);
            }

            auto redir_opt = parse_redirection(err.message);
            if (!redir_opt)
            {
                co_return std::unexpected(err);
            }

            const auto& redir = *redir_opt;

            if (redir.type == RedirType::Moved)
            {
#ifdef UREDIS_LOGS
                usub::ulog::info("RedisClusterClient::command: MOVED slot={} to {}:{} (retry {})",
                                 redir.slot,
                                 redir.host,
                                 redir.port,
                                 attempt + 1);
#endif
                co_await this->apply_moved(redir);
                continue;
            }

            if (redir.type == RedirType::Ask)
            {
#ifdef UREDIS_LOGS
                usub::ulog::info("RedisClusterClient::command: ASK slot={} to {}:{} (retry {})",
                                 redir.slot,
                                 redir.host,
                                 redir.port,
                                 attempt + 1);
#endif
                auto c = co_await this->get_or_create_client_for_node(redir.host, redir.port);
                if (!c)
                {
                    co_return std::unexpected(c.error());
                }
                auto ask_client = c.value();

                std::span<const std::string_view> no_args;
                (void)co_await ask_client->command("ASKING", no_args);

                auto resp2 = co_await ask_client->command(cmd, args);
                if (resp2)
                {
                    co_return resp2;
                }

                auto err2 = resp2.error();
                auto redir2 = parse_redirection(err2.message);
                if (!redir2)
                {
                    co_return std::unexpected(err2);
                }

                if (redir2->type == RedirType::Moved)
                {
                    co_await this->apply_moved(*redir2);
                    continue;
                }

                co_return std::unexpected(err2);
            }

            co_return std::unexpected(err);
        }

        RedisError err{
            RedisErrorCategory::Protocol,
            "RedisClusterClient: too many redirections"
        };
        co_return std::unexpected(err);
    }
} // namespace usub::uredis