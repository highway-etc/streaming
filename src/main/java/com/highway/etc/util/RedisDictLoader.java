package com.highway.etc.util;

import java.util.Map;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPooled;

public class RedisDictLoader implements AutoCloseable {

    private final JedisPooled jedis;

    public RedisDictLoader(String host, int port, String password) {
        if (password != null && !password.isEmpty()) {
            JedisClientConfig cfg = DefaultJedisClientConfig.builder()
                    .password(password)
                    .build();
            this.jedis = new JedisPooled(new HostAndPort(host, port), cfg);
        } else {
            this.jedis = new JedisPooled(host, port);
        }
    }

    public String get(String key) {
        return jedis.get(key);
    }

    public Map<String, String> hgetAll(String key) {
        return jedis.hgetAll(key);
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}
