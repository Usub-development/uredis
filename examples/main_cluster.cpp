#include "uvent/Uvent.h"
#include "uredis/RedisClusterClient.h"
#include <ulog/ulog.h>

using namespace usub::uvent;
using namespace usub::uredis;
namespace task = usub::uvent::task;

using usub::ulog::info;
using usub::ulog::error;

task::Awaitable<void> cluster_example()
{
    RedisClusterConfig cfg;
    cfg.seeds = {
        {"127.0.0.1", 7000},
        {"127.0.0.1", 7001},
    };
    cfg.max_redirections = 8;

    RedisClusterClient cluster{cfg};

    auto c = co_await cluster.connect();
    if (!c)
    {
        const auto& e = c.error();
        error("cluster_example: connect failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    auto r1 = co_await cluster.command("SET", "user:42", "Kirill");
    if (!r1)
    {
        const auto& e = r1.error();
        error("cluster_example: SET failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    auto r2 = co_await cluster.command("GET", "user:42");
    if (!r2)
    {
        const auto& e = r2.error();
        error("cluster_example: GET failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    const RedisValue& v = *r2;
    if (v.is_bulk_string() || v.is_simple_string())
    {
        info("cluster_example: GET user:42 -> '{}'", v.as_string());
    }

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

    info("main(cluster): starting uvent");

    usub::Uvent uvent(4);
    usub::uvent::system::co_spawn(cluster_example());
    uvent.run();

    info("main(cluster): uvent stopped");
    return 0;
}
