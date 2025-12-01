# Redis Cluster Client

uRedis includes a production-grade async **Redis Cluster router** built on top of `RedisClient`:

- CRC16 slot hashing
- Full MOVED / ASK redirection support
- Automatic `CLUSTER SLOTS` discovery
- Individual async `RedisClient` per node
- Lazy connection buildup
- Zero extra dependencies

Cluster mode is sharded across **16384 slots**.  
`RedisClusterClient` selects the correct node for a given key and returns a regular `RedisClient`,
so you keep the full high-level API (`get/set`, hashes, sets, sorted sets, reflection helpers, etc.).

## Architecture

- `RedisClusterClient`
    - `connect()` – runs initial `CLUSTER SLOTS` discovery
    - `get_client_for_key(key)` – returns a connected `RedisClient` for the key's slot
    - `get_random_client()` – returns any connected node (for keyless commands)
- Slot table: `slot_to_node[16384]`
- Dynamic node list with lazy connections
- Redirection handling:
    - **MOVED** → update slot mapping + retry with the new node
    - **ASK** → send `ASKING` to target node + retry once

## Example (basic key routing)

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
        {"127.0.0.1", 7000},
        {"127.0.0.1", 7001},
    };

    RedisClusterClient cluster{cfg};

    auto dc = co_await cluster.connect();
    if (!dc)
        co_return;

    auto client_res = co_await cluster.get_client_for_key("user:1");
    if (!client_res)
        co_return;

    auto client = client_res.value();

    co_await client->set("user:1", "Kirill");
    auto val = co_await client->get("user:1");

    if (val && val->has_value())
    {
        usub::ulog::info("user:1 -> {}", **val);
    }

    co_return;
}
```

## Example (using reflection in Cluster mode)

```cpp
#include "uredis/RedisClusterClient.h"
#include "uredis/RedisReflect.h"

struct User
{
    int64_t id;
    std::string name;
    bool active;
};

task::Awaitable<void> cluster_reflect_example()
{
    RedisClusterConfig cfg;
    cfg.seeds = {
        {"127.0.0.1", 7000},
        {"127.0.0.1", 7001},
    };

    RedisClusterClient cluster{cfg};
    auto dc = co_await cluster.connect();
    if (!dc)
        co_return;

    std::string key = "user:42";

    auto client_res = co_await cluster.get_client_for_key(key);
    if (!client_res)
        co_return;

    auto client = client_res.value();

    User u{.id = 42, .name = "Kirill", .active = true};

    using namespace usub::uredis::reflect;

    auto hset_res = co_await hset_struct(*client, key, u);
    if (!hset_res)
        co_return;

    auto loaded = co_await hget_struct<User>(*client, key);
    if (loaded && loaded->has_value())
    {
        const auto& u2 = **loaded;
        usub::ulog::info("loaded user: id={} name={} active={}",
                         u2.id, u2.name, u2.active);
    }

    co_return;
}
```