#include "uvent/Uvent.h"
#include "uredis/RedisSentinelPool.h"
#include <ulog/ulog.h>

using namespace usub::uvent;
using namespace usub::uredis;
namespace task = usub::uvent::task;

using usub::ulog::info;
using usub::ulog::error;

task::Awaitable<void> example_sentinel_pool()
{
    info("example_sentinel_pool: start");

    RedisSentinelConfig scfg;
    scfg.master_name = "mymaster";
    scfg.sentinels.push_back(
        RedisSentinelNode{
            .host = "127.0.0.1",
            .port = 26379
        });
    scfg.base_redis.db = 0;
    scfg.base_redis.io_timeout_ms = 5000;
    scfg.pool_size = 8;

    RedisSentinelPool pool{scfg};

    auto c = co_await pool.connect();
    if (!c)
    {
        const auto& err = c.error();
        error("example_sentinel_pool: connect failed, category={}, message={}",
              static_cast<int>(err.category), err.message);
        co_return;
    }

    auto r = co_await pool.command("INCRBY", "counter", "1");
    if (!r)
    {
        const auto& err = r.error();
        error("example_sentinel_pool: INCRBY failed, category={}, message={}",
              static_cast<int>(err.category), err.message);
        co_return;
    }

    const RedisValue& v = *r;
    info("example_sentinel_pool: counter -> {}", v.as_integer());
    info("example_sentinel_pool: done");
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

    usub::Uvent uvent(4);
    usub::uvent::system::co_spawn(example_sentinel_pool());
    uvent.run();
}
