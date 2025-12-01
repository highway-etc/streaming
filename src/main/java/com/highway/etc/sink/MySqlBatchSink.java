package com.highway.etc.sink;

import com.highway.etc.common.EnrichedEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySqlBatchSink extends RichSinkFunction<EnrichedEvent> {
    private final String url;
    private final String user;
    private final String password;
    private final String insertSql;
    private final int batchSize;

    private transient Connection conn;
    private transient PreparedStatement ps;
    private transient List<EnrichedEvent> buffer;

    public MySqlBatchSink(String url, String user, String password, String insertSql, int batchSize) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.insertSql = insertSql;
        this.batchSize = batchSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(false);
        ps = conn.prepareStatement(insertSql);
        buffer = new ArrayList<>(batchSize);
    }

    @Override
    public void invoke(EnrichedEvent e, Context context) throws Exception {
        buffer.add(e);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    private void flush() throws Exception {
        for (EnrichedEvent e : buffer) {
            ps.setTimestamp(1, Timestamp.from(e.gcsj));
            ps.setString(2, e.xzqhmc);
            ps.setInt(3, e.adcode);
            ps.setString(4, e.kkmc);
            ps.setInt(5, e.stationId);
            ps.setString(6, e.fxlx);
            ps.setString(7, e.hpzl);
            ps.setString(8, e.hphmMask);
            ps.setString(9, e.clppxh);
            ps.addBatch();
        }
        ps.executeBatch();
        conn.commit();
        buffer.clear();
    }

    @Override
    public void close() throws Exception {
        if (!buffer.isEmpty()) flush();
        if (ps != null) ps.close();
        if (conn != null) conn.close();
    }
}