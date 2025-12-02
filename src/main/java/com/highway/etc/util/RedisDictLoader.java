package com.highway.etc.util;

import redis.clients.jedis.JedisPooled;
import java.util.Map;

public class RedisDictLoader {

    private final JedisPooled jedis;

    public RedisDictLoader(String host, int port, String password) {
        if (password != null && !password.isEmpty()) {
            this.jedis = new JedisPooled(host, port, null, password);
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

    public void close() {
        jedis.close();
    }
}