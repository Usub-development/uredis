//
// Created by kirill on 1/13/26.
//

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

#include <uvent/Uvent.h>
#include <uvent/system/SystemContext.h>
#include <uredis/RedisClusterClient.h>
#include <ulog/ulog.h>

namespace {
using namespace std::chrono_literals;

usub::uvent::task::Awaitable<void> test_reconnect(usub::uredis::RedisClusterClient& redis_client) {
    std::uint64_t ok = 0;
    std::uint64_t fail = 0;

    for (;;) {
        auto res = co_await redis_client.command("HGET", "fx:rates", "USD");

        if (!res) {
            ++fail;
            const auto& e = res.error();
            usub::ulog::error("HGET failed: ({}) ok={} fail={}", e.message, ok, fail);

            co_await usub::uvent::system::this_coroutine::sleep_for(200ms);
            continue;
        }

        const usub::uredis::RedisValue& v = res.value();
        if (v.is_null()) {
            usub::ulog::warn("No rate for USD (key missing?) ok={} fail={}", ok, fail);
        } else {
            ++ok;
            usub::ulog::info("USD:{} ok={} fail={}", v.as_string(), ok, fail);
        }

        co_await usub::uvent::system::this_coroutine::sleep_for(200ms);
    }
}
} // namespace

int main() {
    usub::ulog::ULogInit cfg{
        .trace_path = nullptr,
        .debug_path = nullptr,
        .info_path = nullptr,
        .warn_path = nullptr,
        .error_path = nullptr,
        .critical_path = nullptr,
        .fatal_path = nullptr,
        .flush_interval_ns = 5'000'000'000ULL,
        .queue_capacity = 1024,
        .batch_size = 512,
        .enable_color_stdout = true,
        .max_file_size_bytes = 10 * 1024 * 1024,
        .max_files = 3,
        .json_mode = false,
        .track_metrics = true
    };
    usub::ulog::init(cfg);

    std::string keydb_host = "127.0.0.1";
    std::string keydb_port = "6479";
    std::string keydb_pswd = "devpass";

    usub::Uvent uvent(4);

    usub::uredis::RedisClusterConfig redis_config{
        .seeds = {
            usub::uredis::RedisClusterNode{
                .host = keydb_host,
                .port = static_cast<std::uint16_t>(std::stoi(keydb_port))
            }
        },
        .password = keydb_pswd,
        .connect_timeout_ms = 2000,
        .io_timeout_ms = 2000,
        .max_connections_per_node = 16
    };

    usub::uredis::RedisClusterClient redis_cluster_client{redis_config};

    usub::uvent::system::co_spawn(test_reconnect(redis_cluster_client));
    uvent.run();
    return 0;
}
