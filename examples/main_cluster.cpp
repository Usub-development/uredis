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
        {"127.0.0.1", 7000}
    };
    cfg.max_redirections = 8;
    cfg.max_connections_per_node = 4;

    RedisClusterClient cluster{cfg};

    info("cluster_example: connecting to cluster (discovery + pool prewarm)...");
    auto c = co_await cluster.connect();
    if (!c)
    {
        const auto& e = c.error();
        error("cluster_example: connect failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }
    info("cluster_example: cluster discovery OK, pools prewarmed");

    {
        std::string_view key = "user:42";
        std::string_view value = "Kirill";

        info("cluster_example: SET {} = '{}'", key, value);

        std::array<std::string_view, 2> args{key, value};
        auto set_res = co_await cluster.command(
            "SET",
            std::span<const std::string_view>(args.data(), args.size()));

        if (!set_res)
        {
            const auto& e = set_res.error();
            error("cluster_example: SET failed: category={} message={}",
                  static_cast<int>(e.category), e.message);
            co_return;
        }

        const RedisValue& v = *set_res;
        if (!v.is_simple_string())
        {
            error("cluster_example: SET: unexpected reply type (type={})",
                  static_cast<int>(v.type));
            co_return;
        }

        info("cluster_example: SET reply = '{}'", v.as_string());
    }

    {
        std::string_view key = "user:42";
        info("cluster_example: GET {}", key);

        std::array<std::string_view, 1> args{key};
        auto get_res = co_await cluster.command(
            "GET",
            std::span<const std::string_view>(args.data(), args.size()));

        if (!get_res)
        {
            const auto& e = get_res.error();
            error("cluster_example: GET failed: category={} message={}",
                  static_cast<int>(e.category), e.message);
            co_return;
        }

        const RedisValue& v = *get_res;
        if (v.is_null())
        {
            info("cluster_example: GET {} -> <nil>", key);
        }
        else if (v.is_bulk_string() || v.is_simple_string())
        {
            info("cluster_example: GET {} -> '{}'", key, v.as_string());
        }
        else
        {
            error("cluster_example: GET: unexpected reply type (type={})",
                  static_cast<int>(v.type));
        }
    }

    info("cluster_example: done");
    co_return;
}

task::Awaitable<void> cluster_raw_client_example()
{
    info("cluster_raw_client_example: start");

    RedisClusterConfig cfg;
    cfg.seeds = {
        {"127.0.0.1", 7000}
    };
    cfg.max_redirections = 8;
    cfg.max_connections_per_node = 4;

    RedisClusterClient cluster{cfg};

    auto c = co_await cluster.connect();
    if (!c)
    {
        const auto& e = c.error();
        error("cluster_raw_client_example: connect failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    auto client_res = co_await cluster.get_client_for_key("user:42");
    if (!client_res)
    {
        const auto& e = client_res.error();
        error("cluster_raw_client_example: get_client_for_key failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    auto client = client_res.value();
    info("cluster_raw_client_example: node {}:{}",
         client->config().host, client->config().port);

    auto set_res = co_await client->set("user:42", "Kirill-raw");
    if (!set_res)
    {
        const auto& e = set_res.error();
        error("cluster_raw_client_example: SET failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    auto get_res = co_await client->get("user:42");
    if (!get_res)
    {
        const auto& e = get_res.error();
        error("cluster_raw_client_example: GET failed: category={} message={}",
              static_cast<int>(e.category), e.message);
        co_return;
    }

    if (get_res->has_value())
        info("cluster_raw_client_example: GET user:42 -> '{}'", **get_res);
    else
        info("cluster_raw_client_example: GET user:42 -> <nil>");

    info("cluster_raw_client_example: done");
    co_return;
}

task::Awaitable<void> run_all()
{
    co_await cluster_example();
    co_await cluster_raw_client_example();
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
    usub::uvent::system::co_spawn(run_all());
    uvent.run();

    info("main(cluster): uvent stopped");
    return 0;
}
