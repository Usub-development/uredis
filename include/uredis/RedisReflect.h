#ifndef REDISREFLECT_H
#define REDISREFLECT_H


#include <charconv>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "uvent/Uvent.h"
#include "uredis/RedisClient.h"
#include "uredis/RedisSentinelPool.h"
#include "uredis/RedisClusterClient.h"
#include "ureflect/ureflect_auto.h"

namespace usub::uredis::reflect
{
    namespace task = usub::uvent::task;

    namespace detail
    {
        inline std::string to_str_from_arithmetic(auto v)
        {
            using T = std::remove_cvref_t<decltype(v)>;

            if constexpr (std::is_integral_v<T>)
            {
                char buf[64];
                auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), v);
                if (ec != std::errc{}) return {};
                return std::string(buf, p);
            }
            else if constexpr (std::is_floating_point_v<T>)
            {
                return std::to_string(v);
            }
            else
            {
                static_assert(std::is_same_v<T, void>, "Unsupported arithmetic type");
            }
        }

        inline std::string to_redis_string_impl(const std::string& v) { return v; }

        inline std::string to_redis_string_impl(std::string_view v)
        {
            return std::string(v);
        }

        inline std::string to_redis_string_impl(const char* v)
        {
            return v ? std::string(v) : std::string{};
        }

        inline std::string to_redis_string_impl(bool v)
        {
            return v ? "1" : "0";
        }

        template <class T>
            requires std::is_arithmetic_v<std::remove_cvref_t<T>>
        inline std::string to_redis_string_impl(const T& v)
        {
            return to_str_from_arithmetic(v);
        }

        template <class T>
        inline std::string to_redis_string_impl(const std::optional<T>& opt)
        {
            if (!opt.has_value()) return {};
            return to_redis_string_impl(opt.value());
        }

        template <class T>
        inline std::string to_redis_string(const T& v)
        {
            return to_redis_string_impl(v);
        }

        inline void from_redis_string_impl(const std::string& src, std::string& dst)
        {
            dst = src;
        }

        inline void from_redis_string_impl(const std::string& src, std::string_view& dst)
        {
            dst = std::string_view(src);
        }

        inline void from_redis_string_impl(const std::string& src, bool& dst)
        {
            if (src == "0" || src == "false" || src == "False" || src == "FALSE")
                dst = false;
            else
                dst = true;
        }

        template <class Int>
            requires std::is_integral_v<Int> && (!std::is_same_v<Int, bool>)
        inline void from_redis_string_impl(const std::string& src, Int& dst)
        {
            Int value{};
            auto* begin = src.data();
            auto* end = src.data() + src.size();
            auto [p, ec] = std::from_chars(begin, end, value);
            if (ec == std::errc{})
            {
                dst = value;
            }
        }

        template <class F>
            requires std::is_floating_point_v<F>
        inline void from_redis_string_impl(const std::string& src, F& dst)
        {
            try
            {
                if constexpr (std::is_same_v<F, float>)
                    dst = std::stof(src);
                else if constexpr (std::is_same_v<F, double>)
                    dst = std::stod(src);
                else
                    dst = static_cast<F>(std::stold(src));
            }
            catch (...)
            {
            }
        }

        template <class T>
        inline void from_redis_string_impl(const std::string& src, std::optional<T>& dst)
        {
            if (src.empty())
            {
                dst.reset();
                return;
            }
            T tmp{};
            from_redis_string_impl(src, tmp);
            dst = std::move(tmp);
        }

        template <class T>
        inline void from_redis_string(const std::string& src, T& dst)
        {
            from_redis_string_impl(src, dst);
        }
    } // namespace detail

    template <class T>
        requires std::is_aggregate_v<std::remove_cvref_t<T>>
    task::Awaitable<RedisResult<int64_t>> hset_struct(
        RedisClient& client,
        std::string_view key,
        const T& value)
    {
        using V = std::remove_cvref_t<T>;

        constexpr std::size_t N = ureflect::count_members<V>;
        static_assert(N > 0, "hset_struct: aggregate must have at least one field");

        std::vector<std::string> storage;
        storage.reserve(N * 2);

        std::vector<std::string_view> args;
        args.reserve(1 + N * 2);
        args.push_back(key);

        V& nonconst = const_cast<V&>(value);
        ureflect::for_each_field(nonconst, [&](std::string_view fname, auto& field)
        {
            std::string name_str{fname};
            std::string val_str = detail::to_redis_string(field);

            storage.push_back(std::move(name_str));
            storage.push_back(std::move(val_str));

            args.push_back(storage[storage.size() - 2]);
            args.push_back(storage[storage.size() - 1]);
        });

        auto resp = co_await client.command(
            "HSET",
            std::span<const std::string_view>(args.data(), args.size()));
        if (!resp)
        {
            co_return std::unexpected(resp.error());
        }

        const RedisValue& v = *resp;
        if (v.type != RedisType::Integer)
        {
            RedisError err{RedisErrorCategory::Protocol, "hset_struct: unexpected reply type"};
            co_return std::unexpected(err);
        }

        co_return v.as_integer();
    }

    template <class T>
        requires std::is_aggregate_v<std::remove_cvref_t<T>>
    task::Awaitable<RedisResult<std::optional<T>>> hget_struct(
        RedisClient& client,
        std::string_view key)
    {
        using V = std::remove_cvref_t<T>;

        auto hres = co_await client.hgetall(key);
        if (!hres)
        {
            co_return std::unexpected(hres.error());
        }

        const std::unordered_map<std::string, std::string>& map = hres.value();
        if (map.empty())
        {
            co_return std::optional<V>{};
        }

        V out{};

        ureflect::for_each_field(out, [&](std::string_view fname, auto& field)
        {
            auto it = map.find(std::string(fname));
            if (it == map.end()) return;
            detail::from_redis_string(it->second, field);
        });

        co_return std::optional<V>{std::move(out)};
    }


    template <class T>
        requires std::is_aggregate_v<std::remove_cvref_t<T>>
    task::Awaitable<RedisResult<int64_t>> hset_struct(
        RedisSentinelPool& pool,
        std::string_view key,
        const T& value)
    {
        using V = std::remove_cvref_t<T>;
        constexpr std::size_t N = ureflect::count_members<V>;
        static_assert(N > 0, "hset_struct: aggregate must have at least one field");

        std::vector<std::string> storage;
        storage.reserve(N * 2);

        std::vector<std::string_view> args;
        args.reserve(1 + N * 2);
        args.push_back(key);

        V& nonconst = const_cast<V&>(value);
        ureflect::for_each_field(nonconst, [&](std::string_view fname, auto& field)
        {
            std::string name_str{fname};
            std::string val_str = detail::to_redis_string(field);

            storage.push_back(std::move(name_str));
            storage.push_back(std::move(val_str));

            args.push_back(storage[storage.size() - 2]);
            args.push_back(storage[storage.size() - 1]);
        });

        auto resp = co_await pool.command(
            "HSET",
            std::span<const std::string_view>(args.data(), args.size()));
        if (!resp)
        {
            co_return std::unexpected(resp.error());
        }

        const RedisValue& v = *resp;
        if (v.type != RedisType::Integer)
        {
            RedisError err{RedisErrorCategory::Protocol, "hset_struct(SentinelPool): unexpected reply type"};
            co_return std::unexpected(err);
        }

        co_return v.as_integer();
    }

    template <class T>
        requires std::is_aggregate_v<std::remove_cvref_t<T>>
    task::Awaitable<RedisResult<std::optional<T>>> hget_struct(
        RedisSentinelPool& pool,
        std::string_view key)
    {
        using V = std::remove_cvref_t<T>;

        std::string_view args_arr[1] = {key};
        auto hres = co_await pool.command(
            "HGETALL",
            std::span<const std::string_view>(args_arr, 1));
        if (!hres)
        {
            co_return std::unexpected(hres.error());
        }

        const RedisValue& v = *hres;
        if (v.type == RedisType::Null)
        {
            co_return std::optional<V>{};
        }

        if (v.type != RedisType::Array)
        {
            RedisError err{RedisErrorCategory::Protocol, "hget_struct(SentinelPool): unexpected reply type"};
            co_return std::unexpected(err);
        }

        const auto& arr = v.as_array();
        if (arr.size() & 1)
        {
            RedisError err{RedisErrorCategory::Protocol, "hget_struct(SentinelPool): odd array size"};
            co_return std::unexpected(err);
        }

        std::unordered_map<std::string, std::string> map;
        map.reserve(arr.size() / 2);

        for (std::size_t i = 0; i < arr.size(); i += 2)
        {
            const auto& f = arr[i];
            const auto& val = arr[i + 1];
            if ((!f.is_bulk_string() && !f.is_simple_string()) ||
                (!val.is_bulk_string() && !val.is_simple_string()))
                continue;
            map.emplace(f.as_string(), val.as_string());
        }

        V out{};
        ureflect::for_each_field(out, [&](std::string_view fname, auto& field)
        {
            auto it = map.find(std::string(fname));
            if (it == map.end()) return;
            detail::from_redis_string(it->second, field);
        });

        co_return std::optional<V>{std::move(out)};
    }

       template <class T>
        requires std::is_aggregate_v<std::remove_cvref_t<T>>
    task::Awaitable<RedisResult<int64_t>> hset_struct(
        RedisClusterClient& cluster,
        std::string_view key,
        const T& value)
    {
        using V = std::remove_cvref_t<T>;

        constexpr std::size_t N = ureflect::count_members<V>;
        static_assert(N > 0, "hset_struct: aggregate must have at least one field");

        std::vector<std::string> storage;
        storage.reserve(N * 2);

        std::vector<std::string_view> args;
        args.reserve(1 + N * 2);
        args.push_back(key);

        V& nonconst = const_cast<V&>(value);
        ureflect::for_each_field(nonconst, [&](std::string_view fname, auto& field)
        {
            std::string name_str{fname};
            std::string val_str = detail::to_redis_string(field);

            storage.push_back(std::move(name_str));
            storage.push_back(std::move(val_str));

            args.push_back(storage[storage.size() - 2]);
            args.push_back(storage[storage.size() - 1]);
        });

        auto resp = co_await cluster.command(
            "HSET",
            std::span<const std::string_view>(args.data(), args.size()));
        if (!resp)
        {
            co_return std::unexpected(resp.error());
        }

        const RedisValue& v = *resp;
        if (v.type != RedisType::Integer)
        {
            RedisError err{RedisErrorCategory::Protocol, "hset_struct(Cluster): unexpected reply type"};
            co_return std::unexpected(err);
        }

        co_return v.as_integer();
    }

    template <class T>
        requires std::is_aggregate_v<std::remove_cvref_t<T>>
    task::Awaitable<RedisResult<std::optional<T>>> hget_struct(
        RedisClusterClient& cluster,
        std::string_view key)
    {
        using V = std::remove_cvref_t<T>;

        std::string_view args_arr[1] = {key};
        auto hres = co_await cluster.command(
            "HGETALL",
            std::span<const std::string_view>(args_arr, 1));
        if (!hres)
        {
            co_return std::unexpected(hres.error());
        }

        const RedisValue& v = *hres;
        if (v.type == RedisType::Null)
        {
            co_return std::optional<V>{};
        }

        if (v.type != RedisType::Array)
        {
            RedisError err{RedisErrorCategory::Protocol, "hget_struct(Cluster): unexpected reply type"};
            co_return std::unexpected(err);
        }

        const auto& arr = v.as_array();
        if (arr.size() & 1)
        {
            RedisError err{RedisErrorCategory::Protocol, "hget_struct(Cluster): odd array size"};
            co_return std::unexpected(err);
        }

        std::unordered_map<std::string, std::string> map;
        map.reserve(arr.size() / 2);

        for (std::size_t i = 0; i < arr.size(); i += 2)
        {
            const auto& f = arr[i];
            const auto& val = arr[i + 1];
            if ((!f.is_bulk_string() && !f.is_simple_string()) ||
                (!val.is_bulk_string() && !val.is_simple_string()))
                continue;
            map.emplace(f.as_string(), val.as_string());
        }

        V out{};
        ureflect::for_each_field(out, [&](std::string_view fname, auto& field)
        {
            auto it = map.find(std::string(fname));
            if (it == map.end()) return;
            detail::from_redis_string(it->second, field);
        });

        co_return std::optional<V>{std::move(out)};
    }
} // namespace usub::uredis::reflect

#endif //REDISREFLECT_H
