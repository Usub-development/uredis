# Redis Cluster Client

uRedis includes a production-grade async **Redis Cluster router** built on top of `RedisClient`:

- CRC16 slot hashing
- Full MOVED / ASK redirection support
- Automatic `CLUSTER SLOTS` discovery
- Individual async `RedisClient` per node
- **Per-node async connection pool (MPMC + async mutex)**
- Lazy connection buildup **with optional prewarm during `connect()`**
- Zero extra dependencies

Cluster mode is sharded across **16384 slots**.  
`RedisClusterClient` selects the correct node for a given key and returns a regular `RedisClient`
or executes the command via the node’s **connection pool**, depending on API used.

## Architecture

- `RedisClusterClient`
    - `connect()` – runs initial `CLUSTER SLOTS` discovery  
      **(also prewarms pools up to `max_connections_per_node`)**
    - `command(cmd, args...)` – routes the command through the per-node pool
    - `get_client_for_key(key)` – returns the node’s **main client** (not from pool)
    - `get_random_client()` – same (main client), for keyless commands
- Slot table: `slot_to_node[16384]`
- Dynamic node list with lazy creation of `main_client` and pooled clients
- Redirection handling:
    - **MOVED** → update slot mapping + retry
    - **ASK** → send `ASKING` to target node + retry once

## Connection Pool (New)

Each cluster node now has:

- `main_client` — a persistent connection (not pooled)
- `idle` — MPMC lock-free queue of ready pooled connections
- `live_count` — number of active pooled connections
- `max_connections_per_node` — pool size limit
- On `acquire`:
    - if `idle` is non-empty → reuse
    - else if `live_count < max_connections_per_node` → open new connection
    - else → coroutine sleeps briefly and retries

This ensures high-throughput parallelism for workloads with many concurrent Redis calls.

## Example (basic key routing with cluster.command)

```cpp
#include "uvent/Uvent.h"
#include "uredis/RedisClusterClient.h"
#include <ulog/ulog.h>

using namespace usub::uvent;
using namespace usub::uredis;
namespace task = usub::uvent::task;

task::Awaitable<void> cluster_example()
{
    RedisClusterConfig cfg;
    cfg.seeds = {
        {"127.0.0.1", 7000}
    };
    cfg.max_redirections         = 8;
    cfg.max_connections_per_node = 4;

    RedisClusterClient cluster{cfg};

    auto dc = co_await cluster.connect();
    if (!dc)
        co_return;

    // SET
    std::array<std::string_view, 2> args_set{"user:42", "Kirill"};
    auto set_res = co_await cluster.command("SET",
        std::span<const std::string_view>(args_set));

    if (!set_res)
        co_return;

    // GET
    std::array<std::string_view, 1> args_get{"user:42"};
    auto get_res = co_await cluster.command("GET",
        std::span<const std::string_view>(args_get));

    if (get_res && get_res->is_bulk_string())
        usub::ulog::info("value = {}", get_res->as_string());

    co_return;
}
```

## Example (using raw main_client)

```cpp
task::Awaitable<void> cluster_raw_client_example()
{
    RedisClusterConfig cfg;
    cfg.seeds = {
        {"127.0.0.1", 7000}
    };

    RedisClusterClient cluster{cfg};
    co_await cluster.connect();

    auto client_res = co_await cluster.get_client_for_key("user:42");
    if (!client_res)
        co_return;

    auto cli = client_res.value();  // main-client of the slot's node

    co_await cli->set("user:42", "Kirill");
    auto v = co_await cli->get("user:42");

    if (v && v->has_value())
        usub::ulog::info("raw: {}", **v);

    co_return;
}
```