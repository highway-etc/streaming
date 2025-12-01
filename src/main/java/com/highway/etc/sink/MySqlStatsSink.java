package com.highway.etc.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.time.Instant;
import java.util.Map;

public class MySqlStatsSink extends RichSinkFunction<StatsRecord> {

    public static class StatsRecord {
        public int stationId;
        public Instant windowStart;
        public Instant windowEnd;
        public long count;
        public Map<String, Long> byDir;
        public Map<String, Long> byType;
    }

    private final String url;
    private final String user;
    private final String password;
    private final String insertSql;
    private transient Connection conn;
    private transient PreparedStatement ps;
    private transient ObjectMapper mapper;

    public MySqlStatsSink(String url, String user, String password, String insertSql) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.insertSql = insertSql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(true);
        ps = conn.prepareStatement(insertSql);
        mapper = new ObjectMapper();
    }

    @Override
    public void invoke(StatsRecord r, Context context) throws Exception {
        ps.setInt(1, r.stationId);
        ps.setTimestamp(2, Timestamp.from(r.windowStart));
        ps.setTimestamp(3, Timestamp.from(r.windowEnd));
        ps.setLong(4, r.count);
        ps.setString(5, mapper.writeValueAsString(r.byDir));
        ps.setString(6, mapper.writeValueAsString(r.byType));
        ps.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (ps != null) ps.close();
        if (conn != null) conn.close();
    }
}