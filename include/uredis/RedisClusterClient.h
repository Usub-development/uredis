#ifndef UREDIS_REDISCLUSTERCLIENT_H
#define UREDIS_REDISCLUSTERCLIENT_H

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "uvent/Uvent.h"
#include "uvent/sync/AsyncMutex.h"
#include "uvent/sync/AsyncEvent.h"
#include "uvent/sync/AsyncSemaphore.h"
#include "uvent/utils/datastructures/queue/ConcurrentQueues.h"

#include "uredis/RedisClient.h"
#include "uredis/RedisTypes.h"

namespace usub::uredis {
    namespace task = usub::uvent::task;
    namespace sync = usub::uvent::sync;

    struct RedisClusterNode {
        std::string host;
        std::uint16_t port{6379};
    };

    struct RedisClusterConfig {
        std::vector<RedisClusterNode> seeds;

        std::optional<std::string> username;
        std::optional<std::string> password;

        int connect_timeout_ms{5000};
        int io_timeout_ms{5000};

        int max_redirections{5};
        std::size_t max_connections_per_node{4};
    };

    class RedisClusterClient {
    public:
        explicit RedisClusterClient(RedisClusterConfig cfg);

        task::Awaitable<RedisResult<void> > connect();

        task::Awaitable<RedisResult<RedisValue> > command(
            std::string_view cmd,
            std::span<const std::string_view> args);

        template<typename... Args>
        task::Awaitable<RedisResult<RedisValue> > command(
            std::string_view cmd,
            Args &&... args) {
            std::array<std::string_view, sizeof...(Args)> arr{
                std::string_view{std::forward<Args>(args)}...
            };
            co_return co_await this->command(
                cmd,
                std::span<const std::string_view>(arr.data(), arr.size()));
        }

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
        get_client_for_key(std::string_view key);

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
        get_any_client();

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
        get_client_for_slot(int slot);

    private:
        struct Node {
            RedisConfig cfg;
            std::shared_ptr<RedisClient> main_client;

            usub::queue::concurrent::MPMCQueue<std::shared_ptr<RedisClient> > idle;
            std::atomic<std::size_t> live_count{0};

            sync::AsyncSemaphore idle_sem{0};
            std::atomic<uint32_t> waiters{0};

            Node(RedisConfig cfg_, std::size_t max_pool)
                : cfg(std::move(cfg_))
                  , idle(max_pool)
                  , live_count(0)
                  , idle_sem(0)
                  , waiters(0) {
            }

            void notify_waiters_if_any() noexcept {
                if (waiters.load(std::memory_order_relaxed) > 0)
                    idle_sem.release();
            }
        };

        struct PooledClient {
            std::shared_ptr<Node> node;
            std::shared_ptr<RedisClient> client;
        };

        enum class RedirType {
            None,
            Moved,
            Ask
        };

        struct Redirection {
            RedirType type{RedirType::None};
            int slot{-1};
            std::string host;
            std::uint16_t port{0};
        };

        RedisClusterConfig cfg_;
        std::vector<std::shared_ptr<Node> > nodes_;
        std::array<int, 16384> slot_to_node_{};

        sync::AsyncMutex mutex_;

        sync::AsyncMutex init_mutex_;
        sync::AsyncEvent init_event_{sync::Reset::Manual, false};
        bool init_started_{false};
        bool init_finished_{false};
        std::optional<RedisResult<void> > init_result_;
        bool standalone_mode_{false};

        static std::string_view extract_hash_tag(std::string_view key);

        static std::uint16_t calc_slot(std::string_view key);

        static std::optional<Redirection> parse_redirection(const std::string &msg);

        RedisResult<int> node_index_for_slot_nolock(int slot);

        RedisResult<int> node_index_for_key_nolock(std::string_view key);

        task::Awaitable<RedisResult<void> > initial_discovery();

        task::Awaitable<RedisResult<std::shared_ptr<RedisClient> > >
        get_or_create_main_client_for_node(
            std::string_view host,
            std::uint16_t port);

        task::Awaitable<RedisResult<PooledClient> >
        acquire_client_for_node_locked(
            const std::shared_ptr<Node> &node);

        task::Awaitable<void>
        release_pooled_client(PooledClient &&pc, bool connection_faulty);

        task::Awaitable<RedisResult<PooledClient> >
        acquire_client_for_slot(int slot);

        task::Awaitable<RedisResult<PooledClient> >
        acquire_client_for_any();

        task::Awaitable<RedisResult<PooledClient> >
        acquire_client_for_key(std::string_view key);

        task::Awaitable<void> apply_moved(const Redirection &r);
    };
} // namespace usub::uredis

#endif // UREDIS_REDISCLUSTERCLIENT_H
