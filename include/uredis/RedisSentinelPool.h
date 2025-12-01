#ifndef UREDIS_REDISSENTINELPOOL_H
#define UREDIS_REDISSENTINELPOOL_H

#include <array>
#include <memory>
#include <span>
#include <string_view>

#include "uvent/sync/AsyncMutex.h"
#include "uredis/RedisSentinel.h"
#include "uredis/RedisPool.h"

namespace usub::uredis
{
    namespace task = usub::uvent::task;
    namespace sync = usub::uvent::sync;

    class RedisSentinelPool
    {
    public:
        explicit RedisSentinelPool(RedisSentinelConfig cfg);

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

        const RedisSentinelConfig& config() const { return this->cfg_; }

    private:
        RedisSentinelConfig cfg_;

        std::shared_ptr<RedisPool> pool_;
        bool connected_{false};

        sync::AsyncMutex mutex_;

        task::Awaitable<RedisResult<void>> ensure_connected_locked();
    };
} // namespace usub::uredis

#endif // UREDIS_REDISSENTINELPOOL_H