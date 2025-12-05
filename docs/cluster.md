# Redis Cluster Client

uRedis includes a production-grade async **Redis Cluster router** built on top of `RedisClient`:

* CRC16 slot hashing
* Full MOVED / ASK redirection support
* Automatic `CLUSTER SLOTS` discovery
* Individual async `RedisClient` per node
* **Per-node async connection pool (MPMC + async mutex)**
* Lazy connection buildup **with optional prewarm during `connect()`**
* Zero extra dependencies

Cluster mode is sharded across **16384 slots**.
`RedisClusterClient` selects the correct node for a given key and returns a regular `RedisClient`
or executes the command through the node’s **connection pool**, depending on API used.

---

# Architecture

* `RedisClusterClient`

  * `connect()` – runs initial `CLUSTER SLOTS` discovery
    **(also prewarms pools up to `max_connections_per_node`)**
  * `command(cmd, args...)` – routes the command through the per-node pooled connections
  * `get_client_for_key(key)` – returns the node’s **main client** (not from pool)
  * `get_random_client()` – same (main client), for keyless commands

* Slot table: `slot_to_node[16384]`

* Dynamic node list with lazy creation of:

  * `main_client`
  * pooled clients in `idle` queue

* Redirection handling:

  * **MOVED** → update slot mapping and retry
  * **ASK** → send `ASKING` to target node and retry once

---

# Connection Pool

Each cluster node contains:

* `main_client` — persistent non-pooled connection
* `idle` — MPMC queue for idle pooled connections
* `live_count` — number of created pooled connections
* `max_connections_per_node` — pool capacity

### Acquire logic

1. If queue is not empty → use connection
2. Else if `live_count < max` → open a new pooled connection
3. Else → coroutine briefly sleeps and retries

This ensures excellent parallelism for workloads with high concurrency.

---

# Fallback Mode (Cluster Disabled)

If all seeds return:

```
ERR This instance has cluster support disabled
```

or if `CLUSTER SLOTS` command fails, the client **automatically switches to single-node mode**.

### Behavior in fallback mode

* Slot table is ignored
* Only **one logical node** exists (from the first seed)
* All routing logic collapses to that node
* MOVED/ASK redirections are disabled
* `command()` works exactly the same, using that node’s pool
* `get_client_for_key()` and `get_random_client()` both return the same node
* Pooling remains fully functional

### Logging

When fallback activates, logs include:

```
[W] RedisClusterClient: CLUSTER SLOTS failed on all seeds → entering single-node mode
```

This ensures you always see when cluster autodetection failed.

---

# RedisValue Extensions (New)

RedisValue now has safe helpers for extracting data.

---

## `as_string_optional()`

Returns:

```cpp
std::optional<std::string>
```

### Rules

* Null → `nullopt`
* BulkString or SimpleString → value
* Any other type → `nullopt`

Used for HGET:

```cpp
auto res = co_await cluster.command("HGET", "fx:rates", "KGS");
if (!res) ...
auto s = res->as_string_optional();
```

---

## `as_map()`

Converts array reply from HGETALL into:

```cpp
std::map<std::string, std::string>
```

Skipping invalid entries.

```cpp
auto res = co_await cluster.command("HGETALL", "fx:rates");
auto m = res->as_map();
```

---

## `as_unordered_map()`

Same as `as_map()`, but returns:

```cpp
std::unordered_map<std::string, std::string>
```

Usually preferred for performance.

```cpp
auto m = res->as_unordered_map();
```

---

# Example: key routing with pooled connections

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
    cfg.seeds = { {"127.0.0.1", 7000} };
    cfg.max_redirections         = 8;
    cfg.max_connections_per_node = 4;

    RedisClusterClient cluster{cfg};
    auto dc = co_await cluster.connect();
    if (!dc) co_return;

    std::array<std::string_view, 2> args_set{"user:42", "Kirill"};
    auto set_res = co_await cluster.command("SET", args_set);
    if (!set_res) co_return;

    std::array<std::string_view, 1> args_get{"user:42"};
    auto get_res = co_await cluster.command("GET", args_get);

    if (get_res && get_res->is_bulk_string())
        usub::ulog::info("value = {}", get_res->as_string());

    co_return;
}
```

---

# Example: Using main-client directly

```cpp
task::Awaitable<void> cluster_raw_client_example()
{
    RedisClusterConfig cfg;
    cfg.seeds = { {"127.0.0.1", 7000} };

    RedisClusterClient cluster{cfg};
    co_await cluster.connect();

    auto cli_res = co_await cluster.get_client_for_key("user:42");
    if (!cli_res) co_return;

    auto cli = cli_res.value();

    co_await cli->set("user:42", "Kirill");
    auto v = co_await cli->get("user:42");

    if (v && v->has_value())
        usub::ulog::info("raw: {}", **v);

    co_return;
}
```

---

# Example: Using `as_unordered_map()` to extract rates

```cpp
auto res = co_await cluster.command("HGETALL", "fx:rates");
if (!res) co_return;

auto rates = res->as_unordered_map();
if (rates.empty())
    co_return;

// rates["KGS"], rates["USD"], ...
```