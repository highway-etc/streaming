package com.highway.etc.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.highway.etc.common.EnrichedEvent;

/**
 * JDBC batch sink with checkpoint-triggered flush.
 */
public class MySqlBatchSink extends RichSinkFunction<EnrichedEvent> implements CheckpointedFunction {

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
        // 确保 JDBC Driver 在独立类加载器下被显式注册
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(false);
        ps = conn.prepareStatement(insertSql);
        buffer = new ArrayList<>(batchSize);
    }

    @Override
    public void invoke(EnrichedEvent e, Context context) throws Exception {
        if (e == null) {
            return;
        }
        buffer.add(e);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    private void flush() throws Exception {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }
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
        try {
            ps.executeBatch();
            conn.commit();
        } finally {
            ps.clearBatch();
            buffer.clear();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // stateless
    }
}
