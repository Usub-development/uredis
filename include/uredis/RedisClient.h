#ifndef REDISCLIENT_H
#define REDISCLIENT_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <array>

#include "uvent/Uvent.h"
#include "uvent/sync/AsyncMutex.h"
#include "uvent/utils/buffer/DynamicBuffer.h"

#include "uredis/RedisTypes.h"
#include "uredis/RespParser.h"

namespace usub::uredis {
    namespace net    = usub::uvent::net;
    namespace sync   = usub::uvent::sync;
    namespace task   = usub::uvent::task;

    struct RedisConfig {
        std::string host;
        std::uint16_t port{6379};
        int db{0};

        std::optional<std::string> username;
        std::optional<std::string> password;

        int connect_timeout_ms{5000};
        int io_timeout_ms{5000};
    };

    class RedisClient {
    public:
        explicit RedisClient(RedisConfig cfg);
        ~RedisClient();

        task::Awaitable<RedisResult<void>> connect();

        bool connected() const noexcept { return connected_; }
        bool is_idle() const noexcept;

        task::Awaitable<RedisResult<RedisValue>> command(
            std::string_view cmd,
            std::span<const std::string_view> args);

        template<typename... Args>
        task::Awaitable<RedisResult<RedisValue>> command(std::string_view cmd, Args&&... args) {
            std::array<std::string_view, sizeof...(Args)> arr{ std::string_view{std::forward<Args>(args)}... };
            co_return co_await this->command(cmd, std::span<const std::string_view>(arr.data(), arr.size()));
        }

        task::Awaitable<RedisResult<std::optional<std::string>>> get(std::string_view key);
        task::Awaitable<RedisResult<void>> set(std::string_view key, std::string_view value);
        task::Awaitable<RedisResult<void>> setex(std::string_view key, int ttl_sec, std::string_view value);
        task::Awaitable<RedisResult<int64_t>> del(std::span<const std::string_view> keys);
        task::Awaitable<RedisResult<int64_t>> incrby(std::string_view key, int64_t delta);

        task::Awaitable<RedisResult<int64_t>> hset(std::string_view key, std::string_view field, std::string_view value);
        task::Awaitable<RedisResult<std::optional<std::string>>> hget(std::string_view key, std::string_view field);
        task::Awaitable<RedisResult<std::unordered_map<std::string, std::string>>> hgetall(std::string_view key);

        task::Awaitable<RedisResult<int64_t>> sadd(std::string_view key, std::span<const std::string_view> members);
        task::Awaitable<RedisResult<int64_t>> srem(std::string_view key, std::span<const std::string_view> members);
        task::Awaitable<RedisResult<std::vector<std::string>>> smembers(std::string_view key);

        task::Awaitable<RedisResult<int64_t>> lpush(std::string_view key, std::span<const std::string_view> values);
        task::Awaitable<RedisResult<std::vector<std::string>>> lrange(std::string_view key, int64_t start, int64_t stop);

        task::Awaitable<RedisResult<int64_t>> zadd(
            std::string_view key,
            std::span<const std::pair<std::string, double>> members);

        task::Awaitable<RedisResult<std::vector<std::pair<std::string, double>>>> zrange_with_scores(
            std::string_view key,
            int64_t start,
            int64_t stop);

        const RedisConfig& config() const { return config_; }

    private:
        RedisConfig config_{};
        std::shared_ptr<net::TCPClientSocket> socket_{};

        bool connected_{false};
        bool closing_{false};

        sync::AsyncMutex op_mutex_;

        std::atomic_bool in_flight_{false};

        RespParser parser_{};

        static std::vector<uint8_t> encode_command(std::string_view cmd, std::span<const std::string_view> args);

        void hard_close_socket_unlocked() noexcept;

        task::Awaitable<RedisResult<void>> connect_unlocked();
        task::Awaitable<RedisResult<void>> auth_and_select_unlocked();

        task::Awaitable<RedisResult<RedisValue>> send_and_read_unlocked(
            std::string_view cmd,
            std::span<const std::string_view> args);

        task::Awaitable<RedisResult<RedisValue>> read_one_reply_unlocked();
    };
}

#endif // REDISCLIENT_H
