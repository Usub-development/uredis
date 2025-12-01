#ifndef UREDIS_REDISCLUSTERCLIENT_H
#define UREDIS_REDISCLUSTERCLIENT_H

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "uvent/Uvent.h"
#include "uvent/sync/AsyncMutex.h"
#include "uredis/RedisClient.h"

namespace usub::uredis
{
    namespace task = usub::uvent::task;

    struct RedisClusterNode
    {
        std::string host;
        std::uint16_t port{6379};
    };

    struct RedisClusterConfig
    {
        std::vector<RedisClusterNode> seeds;

        std::optional<std::string> username;
        std::optional<std::string> password;

        int connect_timeout_ms{5000};
        int io_timeout_ms{5000};

        int max_redirections{5};
    };

    class RedisClusterClient
    {
    public:
        explicit RedisClusterClient(RedisClusterConfig cfg);

        task::Awaitable<RedisResult<void>> connect();

        task::Awaitable<RedisResult<RedisValue>> command(
            std::string_view cmd,
            std::span<const std::string_view> args);

        template <typename... Args>
        task::Awaitable<RedisResult<RedisValue>> command(
            std::string_view cmd,
            Args&&... args)
        {
            std::array<std::string_view, sizeof...(Args)> arr{
                std::string_view{std::forward<Args>(args)}...
            };
            co_return co_await this->command(
                cmd,
                std::span<const std::string_view>(arr.data(), arr.size()));
        }

    private:
        struct Node
        {
            RedisConfig cfg;
            std::shared_ptr<RedisClient> client;
        };

        enum class RedirType
        {
            None,
            Moved,
            Ask
        };

        struct Redirection
        {
            RedirType type{RedirType::None};
            int slot{-1};
            std::string host;
            std::uint16_t port{0};
        };

        RedisClusterConfig cfg_;
        std::vector<Node> nodes_;
        std::array<int, 16384> slot_to_node_{};

        usub::uvent::sync::AsyncMutex mutex_;

        task::Awaitable<RedisResult<void>> initial_discovery();

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient>>> get_or_create_client_for_node(
            std::string_view host,
            std::uint16_t port);

        static std::string_view extract_hash_tag(std::string_view key);
        static std::uint16_t calc_slot(std::string_view key);

        static std::optional<Redirection> parse_redirection(const std::string& msg);

        task::Awaitable<void> apply_moved(const Redirection& r);
    };
} // namespace usub::uredis

#endif // UREDIS_REDISCLUSTERCLIENT_H