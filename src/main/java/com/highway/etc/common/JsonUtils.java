package com.highway.etc.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Lightweight helpers for Jackson serialization and configuration loading.
 */
public final class JsonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtils() {
    }

    public static <T> T fromBytes(byte[] b, Class<T> clazz) throws IOException {
        return MAPPER.readValue(b, clazz);
    }

    public static String toJson(Object o) throws IOException {
        return MAPPER.writeValueAsString(o);
    }

    public static <T> List<T> readListFromClasspath(String path, Class<T> clazz) throws IOException {
        try (InputStream is = JsonUtils.class.getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource not found on classpath: " + path);
            }
            return MAPPER.readValue(is,
                    MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
        }
    }

    /**
     * Load a properties file from classpath first, then fall back to the
     * provided filesystem path.
     */
    public static Properties loadProperties(String classpathName, String fallbackPath) throws IOException {
        Properties props = new Properties();
        InputStream classpathStream = JsonUtils.class.getClassLoader().getResourceAsStream(classpathName);
        if (classpathStream != null) {
            try (InputStream in = classpathStream) {
                props.load(in);
                return props;
            }
        }
        try (InputStream in = JsonUtils.class.getClassLoader().getResourceAsStream("/" + classpathName)) {
            if (in != null) {
                props.load(in);
                return props;
            }
        }
        try (InputStream fs = new java.io.FileInputStream(fallbackPath)) {
            props.load(fs);
        }
        if (props.isEmpty()) {
            throw new IllegalStateException("Cannot load properties from classpath or " + fallbackPath);
        }
        return props;
    }

    public static String requireProperty(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required property: " + key);
        }
        return value;
    }

    public static String optionalProperty(Properties props, String key, String defaultValue) {
        return Objects.requireNonNullElse(props.getProperty(key), defaultValue);
    }
}
