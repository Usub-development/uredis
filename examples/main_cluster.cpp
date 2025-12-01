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
    info("cluster_example: start");

    RedisClusterConfig cfg;
    cfg.seeds = {
        {"127.0.0.1", 7000},
        {"127.0.0.1", 7001},
    };
    cfg.max_redirections = 8;

    RedisClusterClient cluster{cfg};

    info("cluster_example: connecting to cluster...");
    auto c = co_await cluster.connect();
    if (!c)
    {
        const auto& e = c.error();
        error("cluster_example: connect failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }
    info("cluster_example: cluster discovery OK");

    auto client_res = co_await cluster.get_client_for_key("user:42");
    if (!client_res)
    {
        const auto& e = client_res.error();
        error("cluster_example: get_client_for_key failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }
    auto client = client_res.value();

    info("cluster_example: using node {}:{} for key 'user:42'",
         client->config().host, client->config().port);

    auto set_res = co_await client->set("user:42", "Kirill");
    if (!set_res)
    {
        const auto& e = set_res.error();
        error("cluster_example: SET failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }
    info("cluster_example: SET user:42 = 'Kirill' OK");

    auto get_res = co_await client->get("user:42");
    if (!get_res)
    {
        const auto& e = get_res.error();
        error("cluster_example: GET failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    const auto& opt = get_res.value();
    if (opt.has_value())
    {
        info("cluster_example: GET user:42 -> '{}'", *opt);
    }
    else
    {
        info("cluster_example: GET user:42 -> <nil>");
    }

    info("cluster_example: done");
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
