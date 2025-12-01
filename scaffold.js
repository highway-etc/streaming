#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const files = [
  { p: 'build.gradle', c: `
plugins {
    id 'java'
    id 'application'
}

group = 'com.highway.etc'
version = '0.1.0'

sourceCompatibility = JavaVersion.VERSION_17
targetCompatibility = JavaVersion.VERSION_17

repositories { mavenCentral() }

dependencies {
    implementation platform("org.apache.flink:flink-bom:1.18.1")
    implementation "org.apache.flink:flink-java"
    implementation "org.apache.flink:flink-streaming-java"
    implementation "org.apache.flink:flink-clients"
    implementation "org.apache.flink:flink-connector-kafka:1.18.1"
    implementation "org.apache.flink:flink-connector-jdbc:1.18.1"

    implementation "com.fasterxml.jackson.core:jackson-databind:2.17.1"
    implementation "com.fasterxml.jackson.core:jackson-core:2.17.1"
    implementation "com.fasterxml.jackson.core:jackson-annotations:2.17.1"

    implementation "org.slf4j:slf4j-api:2.0.12"
    runtimeOnly "org.slf4j:slf4j-simple:2.0.12"

    implementation "mysql:mysql-connector-java:8.0.33"
}

application { mainClass = 'com.highway.etc.job.TrafficStreamingJob' }

tasks.withType(JavaCompile) { options.encoding = 'UTF-8' }

jar { manifest { attributes 'Main-Class': 'com.highway.etc.job.TrafficStreamingJob' } }
`.trim() },
  { p: 'Dockerfile', c: `
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY build/libs/*.jar /app/app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]
`.trim() },
  { p: 'src/main/resources/application.properties', c: `
# Kafka
kafka.bootstrap.servers=localhost:9092
kafka.topic=etc_traffic
kafka.group.id=traffic-consumer

# MySQL
mysql.url=jdbc:mysql://localhost:3306/etc_traffic_db
mysql.user=root
mysql.password=rootpass
mysql.batch.size=500
mysql.insert.sql=INSERT INTO traffic_pass_dev (gcsj,xzqhmc,adcode,kkmc,station_id,fxlx,hpzl,hphm_mask,clppxh,created_at) VALUES (?,?,?,?,?,?,?,?,?,NOW())
mysql.stats.insert.sql=INSERT INTO stats_realtime (station_id,window_start,window_end,cnt,by_dir,by_type) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE cnt=VALUES(cnt),by_dir=VALUES(by_dir),by_type=VALUES(by_type)
mysql.alert.insert.sql=INSERT INTO alert_plate_clone (hphm_mask,first_station_id,second_station_id,time_gap_sec,distance_km,speed_kmh,confidence,created_at) VALUES (?,?,?,?,?,?,?,NOW())

# Stream watermark out-of-orderness
event.out.of.order.ms=120000

# Plate-clone detection
clone.max.speed.kmh=160
clone.min.time.diff.sec=10
`.trim() },
  { p: 'src/main/resources/stations.json', c: `
[
  { "stationId": 101, "name": "Station-A", "lat": 23.129, "lon": 113.264 },
  { "stationId": 102, "name": "Station-B", "lat": 22.543, "lon": 114.057 },
  { "stationId": 103, "name": "Station-C", "lat": 23.392, "lon": 116.700 }
]
`.trim() },
  { p: 'src/main/java/com/highway/etc/common/Event.java', c: `
package com.highway.etc.common;

import java.time.Instant;

public class Event {
    public long gcxh;
    public String xzqhmc;
    public int adcode;
    public String kkmc;
    public int stationId;
    public String fxlx;
    public Instant gcsj;
    public String hpzl;
    public String hphm;
    public String hphmMask;
    public String clppxh;
}
`.trim() },
  { p: 'src/main/java/com/highway/etc/common/EnrichedEvent.java', c: `
package com.highway.etc.common;

import java.util.Map;

public class EnrichedEvent extends Event {
    public Map<String,Object> tags;
}
`.trim() },
  { p: 'src/main/java/com/highway/etc/common/StationInfo.java', c: `
package com.highway.etc.common;

public class StationInfo {
    public int stationId;
    public String name;
    public double lat;
    public double lon;
}
`.trim() },
  { p: 'src/main/java/com/highway/etc/common/Alert.java', c: `
package com.highway.etc.common;

import java.time.Instant;

public class Alert {
    public String hphmMask;
    public int firstStationId;
    public int secondStationId;
    public long timeGapSec;
    public double distanceKm;
    public double speedKmh;
    public double confidence;
    public Instant generatedAt;
}
`.trim() },
  { p: 'src/main/java/com/highway/etc/common/JsonUtils.java', c: `
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
`.trim() },
  { p: 'src/main/java/com/highway/etc/util/DistanceUtil.java', c: `
package com.highway.etc.util;

public class DistanceUtil {
    public static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371.0;
        double dLat = Math.toRadians(lat2-lat1);
        double dLon = Math.toRadians(lon2-lon1);
        double a = Math.sin(dLat/2)*Math.sin(dLat/2) +
                   Math.cos(Math.toRadians(lat1))*Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon/2)*Math.sin(dLon/2);
        double c = 2*Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R*c;
    }
}
`.trim() },
  { p: 'src/main/java/com/highway/etc/sink/MySqlBatchSink.java', c: `
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
        if (buffer.size() >= batchSize) flush();
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
`.trim() },
  { p: 'src/main/java/com/highway/etc/sink/MySqlStatsSink.java', c: `
package com.highway.etc.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.time.Instant;
import java.util.Map;

public class MySqlStatsSink extends RichSinkFunction<MySqlStatsSink.StatsRecord> {

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
`.trim() },
  { p: 'src/main/java/com/highway/etc/sink/MySqlAlertSink.java', c: `
package com.highway.etc.sink;

import com.highway.etc.common.Alert;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;

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
        if (ps != null) ps.close();
        if (conn != null) conn.close();
    }
}
`.trim() },
  { p: 'src/main/java/com/highway/etc/job/TrafficStreamingJob.java', c: `
package com.highway.etc.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highway.etc.common.EnrichedEvent;
import com.highway.etc.common.Event;
import com.highway.etc.sink.MySqlBatchSink;
import com.highway.etc.sink.MySqlStatsSink;
import com.highway.etc.sink.MySqlStatsSink.StatsRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.FileInputStream;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;

public class TrafficStreamingJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/application.properties")) {
            props.load(fis);
        }
        final String kafkaServers = props.getProperty("kafka.bootstrap.servers");
        final String topic = props.getProperty("kafka.topic");
        final String mysqlUrl = props.getProperty("mysql.url");
        final String mysqlUser = props.getProperty("mysql.user");
        final String mysqlPwd = props.getProperty("mysql.password");
        final int batchSize = Integer.parseInt(props.getProperty("mysql.batch.size","500"));
        final String insertSql = props.getProperty("mysql.insert.sql");
        final String statsInsertSql = props.getProperty("mysql.stats.insert.sql");
        final long watermarkOutOfOrderMs = Long.parseLong(props.getProperty("event.out.of.order.ms","120000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(2000);

        KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics(topic)
                .setGroupId(props.getProperty("kafka.group.id","traffic-consumer"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new EventDeserializer())
                .build();

        DataStream<Event> raw = env.fromSource(kafkaSource,
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(java.time.Duration.ofMillis(watermarkOutOfOrderMs))
                        .withTimestampAssigner((e, ts) -> e.gcsj.toEpochMilli()),
                "kafka-source");

        DataStream<EnrichedEvent> enriched = raw.map(e -> {
            EnrichedEvent ne = new EnrichedEvent();
            ne.gcxh = e.gcxh;
            ne.xzqhmc = e.xzqhmc == null ? "" : e.xzqhmc;
            ne.adcode = e.adcode;
            ne.kkmc = e.kkmc == null ? "" : e.kkmc;
            ne.stationId = e.stationId;
            ne.fxlx = e.fxlx == null ? "" : e.fxlx;
            ne.gcsj = e.gcsj;
            ne.hpzl = e.hpzl == null ? "" : e.hpzl;
            ne.hphm = e.hphm == null ? "" : e.hphm;
            ne.hphmMask = e.hphmMask == null && e.hphm != null ? e.hphm.substring(0, Math.min(4,e.hphm.length())) + "****" : e.hphmMask;
            ne.clppxh = e.clppxh == null ? "" : e.clppxh;
            ne.tags = Collections.emptyMap();
            return ne;
        }).name("enrich-map");

        enriched.addSink(new MySqlBatchSink(mysqlUrl, mysqlUser, mysqlPwd, insertSql, batchSize))
                .name("mysql-batch-sink");

        DataStream<StatsRecord> stats = enriched
                .keyBy((KeySelector<EnrichedEvent, Integer>) e -> e.stationId)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<EnrichedEvent, StatsRecord, Integer, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                    @Override
                    public void process(Integer key, Context context, Iterable<EnrichedEvent> elements, org.apache.flink.util.Collector<StatsRecord> out) {
                        long cnt = 0;
                        Map<String, Long> dirMap = new HashMap<>();
                        Map<String, Long> typeMap = new HashMap<>();
                        for (EnrichedEvent e : elements) {
                            cnt++;
                            dirMap.merge(e.fxlx, 1L, Long::sum);
                            typeMap.merge(e.hpzl, 1L, Long::sum);
                        }
                        StatsRecord r = new StatsRecord();
                        r.stationId = key;
                        r.windowStart = Instant.ofEpochMilli(context.window().getStart());
                        r.windowEnd = Instant.ofEpochMilli(context.window().getEnd());
                        r.count = cnt;
                        r.byDir = dirMap;
                        r.byType = typeMap;
                        out.collect(r);
                    }
                }).name("window-agg-stats");

        stats.addSink(new MySqlStatsSink(mysqlUrl, mysqlUser, mysqlPwd, statsInsertSql))
                .name("mysql-stats-sink");

        env.execute("TrafficStreamingJob");
    }

    public static class EventDeserializer extends AbstractDeserializationSchema<Event> {
        private final ObjectMapper mapper = new ObjectMapper();
        @Override
        public Event deserialize(byte[] message) throws Exception {
            JsonNode n = mapper.readTree(message);
            Event e = new Event();
            e.gcxh = n.path("gcxh").asLong();
            e.xzqhmc = n.path("xzqhmc").asText(null);
            e.adcode = n.path("adcode").asInt(0);
            e.kkmc = n.path("kkmc").asText(null);
            e.stationId = n.path("station_id").asInt(0);
            e.fxlx = n.path("fxlx").asText(null);
            String gcsjStr = n.path("gcsj").asText(null);
            try { e.gcsj = Instant.parse(gcsjStr); } catch (Exception ex) { e.gcsj = Instant.now(); }
            e.hpzl = n.path("hpzl").asText(null);
            e.hphm = n.path("hphm").asText(null);
            e.hphmMask = n.path("hphm_mask").asText(null);
            e.clppxh = n.path("clppxh").asText(null);
            return e;
        }
    }
}
`.trim() },
  { p: 'src/main/java/com/highway/etc/job/PlateCloneDetectionJob.java', c: `
package com.highway.etc.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highway.etc.common.Alert;
import com.highway.etc.common.Event;
import com.highway.etc.common.JsonUtils;
import com.highway.etc.common.StationInfo;
import com.highway.etc.sink.MySqlAlertSink;
import com.highway.etc.util.DistanceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class PlateCloneDetectionJob {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/application.properties")) {
            props.load(fis);
        }
        final String kafkaServers = props.getProperty("kafka.bootstrap.servers");
        final String topic = props.getProperty("kafka.topic");
        final String mysqlUrl = props.getProperty("mysql.url");
        final String mysqlUser = props.getProperty("mysql.user");
        final String mysqlPwd = props.getProperty("mysql.password");
        final String alertInsertSql = props.getProperty("mysql.alert.insert.sql");
        final long watermarkOutOfOrderMs = Long.parseLong(props.getProperty("event.out.of.order.ms","120000"));
        final double maxSpeed = Double.parseDouble(props.getProperty("clone.max.speed.kmh","160"));
        final long minDiffSec = Long.parseLong(props.getProperty("clone.min.time.diff.sec","10"));

        List<StationInfo> stationInfos = JsonUtils.readListFromClasspath("/stations.json", StationInfo.class);
        Map<Integer, StationInfo> stationMap = new HashMap<>();
        for (StationInfo si : stationInfos) stationMap.put(si.stationId, si);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(2000);

        KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics(topic)
                .setGroupId("plate-clone-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new BasicEventDeserializer())
                .build();

        DataStream<Event> stream = env.fromSource(kafkaSource,
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofMillis(watermarkOutOfOrderMs))
                        .withTimestampAssigner((e, ts) -> e.gcsj.toEpochMilli()),
                "kafka-source-clone");

        stream
            .keyBy(e -> e.hphm == null ? "NULL" : e.hphm)
            .process(new CloneDetectProcessFunction(stationMap, maxSpeed, minDiffSec))
            .addSink(new MySqlAlertSink(mysqlUrl, mysqlUser, mysqlPwd, alertInsertSql))
            .name("mysql-alert-sink");

        env.execute("PlateCloneDetectionJob");
    }

    public static class BasicEventDeserializer extends AbstractDeserializationSchema<Event> {
        private final ObjectMapper mapper = new ObjectMapper();
        @Override
        public Event deserialize(byte[] message) throws Exception {
            JsonNode n = mapper.readTree(message);
            Event e = new Event();
            e.gcxh = n.path("gcxh").asLong();
            e.hphm = n.path("hphm").asText(null);
            e.hphmMask = n.path("hphm_mask").asText(null);
            e.stationId = n.path("station_id").asInt(0);
            e.gcsj = Instant.parse(n.path("gcsj").asText(Instant.now().toString()));
            e.clppxh = n.path("clppxh").asText(null);
            return e;
        }
    }

    public static class CloneDetectProcessFunction
            extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, Event, Alert> {

        private final Map<Integer, StationInfo> stationMap;
        private final double maxSpeedKmh;
        private final long minDiffSec;
        private transient ListState<Event> historyState;

        public CloneDetectProcessFunction(Map<Integer, StationInfo> stationMap, double maxSpeedKmh, long minDiffSec) {
            this.stationMap = stationMap;
            this.maxSpeedKmh = maxSpeedKmh;
            this.minDiffSec = minDiffSec;
        }

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttl = StateTtlConfig
                    .newBuilder(Time.minutes(30))
                    .cleanupFullSnapshot()
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
            ListStateDescriptor<Event> desc = new ListStateDescriptor<>("plate-history", Event.class);
            desc.enableTimeToLive(ttl);
            historyState = getRuntimeContext().getListState(desc);
        }

        @Override
        public void processElement(Event current, Context ctx, org.apache.flink.util.Collector<Alert> out) throws Exception {
            if (current.hphm == null || current.stationId == 0) return;

            List<Event> prevs = new ArrayList<>();
            for (Event e : historyState.get()) prevs.add(e);

            prevs.add(current);
            historyState.update(prevs);

            StationInfo cur = stationMap.get(current.stationId);
            if (cur == null) return;

            for (Event prev : prevs) {
                if (prev == current) continue;
                long diffSec = Math.abs(current.gcsj.getEpochSecond() - prev.gcsj.getEpochSecond());
                if (diffSec < minDiffSec) continue;
                StationInfo pv = stationMap.get(prev.stationId);
                if (pv == null || prev.stationId == current.stationId) continue;

                double distanceKm = DistanceUtil.haversine(cur.lat, cur.lon, pv.lat, pv.lon);
                double hours = diffSec / 3600.0;
                double speedKmh = hours == 0 ? Double.POSITIVE_INFINITY : distanceKm / hours;

                if (speedKmh > maxSpeedKmh) {
                    Alert a = new Alert();
                    a.hphmMask = current.hphmMask != null ? current.hphmMask :
                            (current.hphm == null ? "UNKNOWN" : current.hphm.substring(0, Math.min(4,current.hphm.length())) + "****");
                    if (current.gcsj.isAfter(prev.gcsj)) {
                        a.firstStationId = prev.stationId;
                        a.secondStationId = current.stationId;
                        a.timeGapSec = diffSec;
                    } else {
                        a.firstStationId = current.stationId;
                        a.secondStationId = prev.stationId;
                        a.timeGapSec = diffSec;
                    }
                    a.distanceKm = distanceKm;
                    a.speedKmh = speedKmh;
                    a.confidence = Math.min(1.0, (speedKmh - maxSpeedKmh) / maxSpeedKmh + 0.5);
                    a.generatedAt = Instant.now();
                    out.collect(a);
                }
            }
        }
    }
}
`.trim() },
];

function ensureDir(filePath) {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
}

for (const f of files) {
  ensureDir(f.p);
  fs.writeFileSync(f.p, f.c, 'utf8');
  console.log('Created', f.p);
}

console.log('\\nProject scaffold complete.');