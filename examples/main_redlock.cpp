#include "uvent/Uvent.h"
#include "uredis/RedisClient.h"
#include "uredis/RedisRedlock.h"
#include <ulog/ulog.h>

using namespace usub::uvent;
using namespace usub::uredis;

namespace task   = usub::uvent::task;

using usub::ulog::info;
using usub::ulog::warn;
using usub::ulog::error;

task::Awaitable<void> worker_coro(
    int id,
    std::shared_ptr<RedisRedlock> redlock,
    std::shared_ptr<RedisClient> data_client,
    int iterations)
{
    using namespace std::chrono_literals;

    info("worker[{}]: start, iterations={}", id, iterations);

    for (int i = 0; i < iterations; ++i)
    {
        auto lock_res = co_await redlock->lock("lock:demo:counter");
        if (!lock_res)
        {
            const auto& err = lock_res.error();
            warn("worker[{}]: lock failed (iter={}): category={}, message={}",
                 id, i, static_cast<int>(err.category), err.message);
            co_await system::this_coroutine::sleep_for(50ms);
            continue;
        }

        auto handle = lock_res.value();

        auto g = co_await data_client->get("demo:counter");
        if (!g)
        {
            const auto& err = g.error();
            error("worker[{}]: GET demo:counter failed: category={}, message={}",
                  id, static_cast<int>(err.category), err.message);
            co_await redlock->unlock(handle);
            co_return;
        }

        int64_t current = 0;
        if (g.value().has_value())
        {
            try
            {
                current = std::stoll(g.value().value());
            }
            catch (...)
            {
                warn("worker[{}]: invalid counter value='{}', reset to 0",
                     id, g.value().value());
                current = 0;
            }
        }

        int64_t next = current + 1;
        std::string next_str = std::to_string(next);

        auto s = co_await data_client->set("demo:counter", next_str);
        if (!s)
        {
            const auto& err = s.error();
            error("worker[{}]: SET demo:counter failed: category={}, message={}",
                  id, static_cast<int>(err.category), err.message);
            co_await redlock->unlock(handle);
            co_return;
        }

        info("worker[{}]: iter={} counter {} -> {}",
             id, i, current, next);

        auto u = co_await redlock->unlock(handle);
        if (!u)
        {
            const auto& err = u.error();
            warn("worker[{}]: unlock failed: category={}, message={}",
                 id, static_cast<int>(err.category), err.message);
        }

        co_await system::this_coroutine::sleep_for(10ms);
    }

    info("worker[{}]: done", id);
    co_return;
}

task::Awaitable<void> redlock_demo()
{
    info("redlock_demo: start");

    RedlockConfig rcfg;
    rcfg.ttl_ms        = 2000;
    rcfg.retry_count   = 5;
    rcfg.retry_delay_ms = 100;

    {
        RedisConfig n1;
        n1.host = "127.0.0.1";
        n1.port = 15100;

        RedisConfig n2 = n1;
        n2.port = 15101;

        RedisConfig n3 = n1;
        n3.port = 15102;

        rcfg.nodes.push_back(n1);
        rcfg.nodes.push_back(n2);
        rcfg.nodes.push_back(n3);
    }

    auto redlock = std::make_shared<RedisRedlock>(rcfg);
    auto redlock_conn = co_await redlock->connect_all();
    if (!redlock_conn)
    {
        const auto& err = redlock_conn.error();
        error("redlock_demo: connect_all failed: category={}, message={}",
              static_cast<int>(err.category), err.message);
        co_return;
    }
    info("redlock_demo: redlock nodes connected");

    RedisConfig data_cfg = rcfg.nodes.front();
    auto data_client = std::make_shared<RedisClient>(data_cfg);
    auto dc = co_await data_client->connect();
    if (!dc)
    {
        const auto& err = dc.error();
        error("redlock_demo: data_client connect failed: category={}, message={}",
              static_cast<int>(err.category), err.message);
        co_return;
    }
    info("redlock_demo: data_client connected");

    auto r = co_await data_client->set("demo:counter", "0");
    if (!r)
    {
        const auto& err = r.error();
        error("redlock_demo: reset demo:counter failed: category={}, message={}",
              static_cast<int>(err.category), err.message);
        co_return;
    }
    info("redlock_demo: demo:counter reset to 0");

    const int workers   = 4;
    const int iterations = 10;

    for (int i = 0; i < workers; ++i)
    {
        system::co_spawn(
            worker_coro(i, redlock, data_client, iterations));
    }

    using namespace std::chrono_literals;
    co_await system::this_coroutine::sleep_for(3s);

    auto g = co_await data_client->get("demo:counter");
    if (!g)
    {
        const auto& err = g.error();
        error("redlock_demo: final GET failed: category={}, message={}",
              static_cast<int>(err.category), err.message);
        co_return;
    }

    if (g.value().has_value())
    {
        info("redlock_demo: final demo:counter = '{}'", g.value().value());
    }
    else
    {
        warn("redlock_demo: final demo:counter -> (nil)");
    }

    info("redlock_demo: done");
    co_return;
}

int main()
{
    usub::ulog::ULogInit log_cfg{
        .trace_path = nullptr,
        .debug_path = nullptr,
        .info_path = nullptr,
        .warn_path = nullptr,
        .error_path = nullptr,
        .flush_interval_ns = 2'000'000ULL,
        .queue_capacity = 16384,
        .batch_size = 512,
        .enable_color_stdout = true,
        .max_file_size_bytes = 10 * 1024 * 1024,
        .max_files = 3,
        .json_mode = false,
        .track_metrics = true
    };

    usub::ulog::init(log_cfg);

    info("main(redlock): starting uvent");

    usub::Uvent uvent(4);
    system::co_spawn(redlock_demo());
    uvent.run();

    info("main(redlock): uvent stopped");
    return 0;
}