package com.highway.etc.util;

import redis.clients.jedis.Jedis;
import java.util.Optional;

public class RedisDictLoader {
    private final boolean enabled;
    private final String host;
    private final int port;

    public RedisDictLoader(boolean enabled, String host, int port) {
        this.enabled = enabled;
        this.host = host;
        this.port = port;
    }

    public Optional<String> get(String key) {
        if (!enabled) return Optional.empty();
        try (Jedis j = new Jedis(host, port)) {
            return Optional.ofNullable(j.get(key));
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}