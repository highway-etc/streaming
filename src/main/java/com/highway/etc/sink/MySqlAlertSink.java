package com.highway.etc.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.highway.etc.common.Alert;

public class MySqlAlertSink extends RichSinkFunction<Alert> {

    private final String url;
    private final String user;
    private final String password;
    private final String insertSql;
    private transient Connection conn;
    private transient PreparedStatement ps;

    public MySqlAlertSink(String url, String user, String password, String insertSql) {
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
    }

    @Override
    public void invoke(Alert a, Context context) throws Exception {
        if (a == null) {
            return;
        }
        ps.setString(1, a.hphmMask);
        ps.setInt(2, a.firstStationId);
        ps.setInt(3, a.secondStationId);
        ps.setLong(4, a.timeGapSec);
        ps.setDouble(5, a.distanceKm);
        ps.setDouble(6, a.speedKmh);
        ps.setDouble(7, a.confidence);
        ps.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (ps != null) {
            ps.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
