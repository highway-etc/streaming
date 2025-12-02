package com.highway.etc.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highway.etc.common.Alert;
import com.highway.etc.common.Event;
import com.highway.etc.common.StationInfo;
import com.highway.etc.common.JsonUtils;
import com.highway.etc.sink.MySqlAlertSink;
import com.highway.etc.util.DistanceUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.util.Collector;

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

        // 加载站点信息
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
            .keyBy(e -> e.hphm == null ? "NULL" : e.hphm)  // 按原始车牌键控
            .process(new CloneDetectProcessFunction(stationMap, maxSpeed, minDiffSec))
            .addSink(new MySqlAlertSink(mysqlUrl, mysqlUser, mysqlPwd, alertInsertSql))
            .name("mysql-alert-sink");

        env.execute("PlateCloneDetectionJob");
    }

    // 反序列化
    public static class BasicEventDeserializer extends AbstractDeserializationSchema<Event> {
        private final ObjectMapper mapper = new ObjectMapper();
        @Override
        public Event deserialize(byte[] message) {
            try {
                JsonNode n = mapper.readTree(message);
                Event e = new Event();
                e.gcxh = n.path("gcxh").asLong();
                e.hphm = n.path("hphm").asText(null);
                e.hphmMask = n.path("hphm_mask").asText(null);
                e.stationId = n.path("station_id").asInt(0);
                e.gcsj = Instant.parse(n.path("gcsj").asText(Instant.now().toString()));
                e.clppxh = n.path("clppxh").asText(null);
                return e;
            } catch (Exception ex) {
                return null;
            }
            
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
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .cleanupFullSnapshot()
                    .build();
            ListStateDescriptor<Event> desc = new ListStateDescriptor<>("plate-history", Event.class);
            desc.enableTimeToLive(ttl);
            historyState = getRuntimeContext().getListState(desc);
        }

        @Override
        public void processElement(Event current, Context ctx, Collector<Alert> out) throws Exception {
            if (current.hphm == null || current.stationId == 0) return;
            List<Event> prevs = new ArrayList<>();
            for (Event e : historyState.get()) prevs.add(e);

            // 新事件加入
            prevs.add(current);
            historyState.update(prevs);

            StationInfo currentStation = stationMap.get(current.stationId);
            if (currentStation == null) return;

            for (Event prev : prevs) {
                if (prev == current) continue;
                long diffSec = Math.abs(current.gcsj.getEpochSecond() - prev.gcsj.getEpochSecond());
                if (diffSec < minDiffSec) continue; // 时间太近，忽略
                StationInfo prevStation = stationMap.get(prev.stationId);
                if (prevStation == null || prev.stationId == current.stationId) continue;

                double distanceKm = DistanceUtil.haversine(
                        currentStation.lat, currentStation.lon,
                        prevStation.lat, prevStation.lon);

                double hours = diffSec / 3600.0;
                double speedKmh = hours == 0 ? Double.POSITIVE_INFINITY : distanceKm / hours;

                if (speedKmh > maxSpeedKmh) {
                    Alert alert = new Alert();
                    alert.hphmMask = current.hphmMask != null ? current.hphmMask :
                            (current.hphm == null ? "UNKNOWN" : current.hphm.substring(0, Math.min(4,current.hphm.length())) + "****");
                    // 按时间先后选 first / second
                    if (current.gcsj.isAfter(prev.gcsj)) {
                        alert.firstStationId = prev.stationId;
                        alert.secondStationId = current.stationId;
                        alert.timeGapSec = diffSec;
                    } else {
                        alert.firstStationId = current.stationId;
                        alert.secondStationId = prev.stationId;
                        alert.timeGapSec = diffSec;
                    }
                    alert.distanceKm = distanceKm;
                    alert.speedKmh = speedKmh;
                    alert.confidence = Math.min(1.0, (speedKmh - maxSpeedKmh) / maxSpeedKmh + 0.5); // 简单评分
                    alert.generatedAt = Instant.now();
                    out.collect(alert);
                }
            }
        }
    }
}