package com.highway.etc.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.List;

public class JsonUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static <T> T fromBytes(byte[] b, Class<T> clazz) throws Exception {
        return MAPPER.readValue(b, clazz);
    }
    public static <T> List<T> readListFromClasspath(String path, Class<T> clazz) throws Exception {
        InputStream is = JsonUtils.class.getResourceAsStream(path);
        return MAPPER.readValue(is,
                MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
    }
    public static String toJson(Object o) throws Exception {
        return MAPPER.writeValueAsString(o);
    }
}