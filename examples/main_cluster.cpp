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
    auto res = co_await cluster.command("HGET", "fx:rates", "KGS");

    if (!res)
    {
        const auto& e = res.error();
        error("HGET failed: {} {}", static_cast<int>(e.category), e.message);
        co_return;
    }

    const auto& v = *res;
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
    cfg.seeds = {{"127.0.0.1", 7000}};
    cfg.max_redirections         = 8;
    cfg.max_connections_per_node = 4;

    usub::uredis::RedisClusterClient cluster{cfg};

    co_await bootstrap(cluster);

    co_await example_after_bootstrap(cluster);

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
