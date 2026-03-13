#include "uvent/Uvent.h"
#include "uredis/RedisClusterClient.h"
#include <ulog/ulog.h>

using namespace usub::uvent;
namespace task = usub::uvent::task;

using usub::ulog::info;
using usub::ulog::error;

task::Awaitable<void> bootstrap(usub::uredis::RedisClusterClient& cluster)
{
    info("bootstrap: connecting Redis cluster (discovery + pool prewarm)...");
    auto r = co_await cluster.connect();
    if (!r)
    {
        const auto& e = r.error();
        error("bootstrap: cluster connect failed: category={} msg={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    info("bootstrap: OK");
    co_return;
}

task::Awaitable<void> example_after_bootstrap(usub::uredis::RedisClusterClient& cluster)
{
    info("writing fx:rates[KGS] ...");

    auto set_res = co_await cluster.command("HSET", "fx:rates", "KGS", "1.123456");
    if (!set_res)
    {
        const auto& e = set_res.error();
        error("HSET failed: category={} msg={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    info("HSET OK, sleeping before read...");
    co_await system::this_coroutine::sleep_for(std::chrono::milliseconds(1000));

    info("reading fx:rates[KGS] ...");

    auto get_res = co_await cluster.command("HGET", "fx:rates", "KGS");
    if (!get_res)
    {
        const auto& e = get_res.error();
        error("HGET failed: category={} msg={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    const auto& v = *get_res;
    if (v.is_null())
    {
        info("KGS not found");
        co_return;
    }

    info("KGS rate = {}", v.as_string());
    co_return;
}

task::Awaitable<void> run_all()
{
    usub::uredis::RedisClusterConfig cfg;
    cfg.seeds = {
        {"127.0.0.1", 7000},
        {"127.0.0.1", 7001},
        {"127.0.0.1", 7002},
    };
    cfg.password                 = "redispass";
    cfg.max_redirections         = 8;
    cfg.max_connections_per_node = 4;

    usub::uredis::RedisClusterClient cluster{cfg};

    co_await bootstrap(cluster);
    usub::ulog::debug("before example");
    co_await example_after_bootstrap(cluster);
    usub::ulog::debug("after example");

    co_return;
}

int main()
{
    usub::ulog::ULogInit log_cfg{
        .enable_color_stdout = true,
        .track_metrics       = true
    };
    usub::ulog::init(log_cfg);

    usub::Uvent uvent(4);
    system::co_spawn(run_all());
    uvent.run();
}
