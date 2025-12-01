#include "uvent/Uvent.h"
#include "uredis/RedisSentinelPool.h"
#include <ulog/ulog.h>

using namespace usub::uvent;
using namespace usub::uredis;
namespace task = usub::uvent::task;

using usub::ulog::info;
using usub::ulog::error;

task::Awaitable<void> example_sentinel()
{
    info("example_sentinel: start");

    RedisSentinelConfig scfg;
    scfg.master_name = "mymaster";
    scfg.sentinels = {
        {"127.0.0.1", 26379}
    };
    scfg.base_redis.db = 0;
    scfg.base_redis.io_timeout_ms = 5000;

    RedisSentinelPool sp{scfg};

    info("example_sentinel: resolving master via sentinel...");
    auto c = co_await sp.connect();
    if (!c)
    {
        const auto& err = c.error();
        error("example_sentinel: connect failed: category={} message={}",
              static_cast<int>(err.category), err.message);
        co_return;
    }

    auto client_res = co_await sp.get_master_client();
    if (!client_res)
    {
        const auto& err = client_res.error();
        error("example_sentinel: get_master_client failed: category={} message={}",
              static_cast<int>(err.category), err.message);
        co_return;
    }

    auto client = client_res.value();
    info("example_sentinel: using master {}:{}", client->config().host, client->config().port);

    auto r = co_await client->incrby("counter", 1);
    if (!r)
    {
        const auto& err = r.error();
        error("example_sentinel: INCRBY failed: category={} message={}",
              static_cast<int>(err.category), err.message);
        co_return;
    }

    info("example_sentinel: counter = {}", r.value());
    info("example_sentinel: done");

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
    usub::uvent::system::co_spawn(example_sentinel());
    uvent.run();

    return 0;
}
