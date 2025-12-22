package com.highway.etc.job;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highway.etc.common.Alert;
import com.highway.etc.common.Event;
import com.highway.etc.common.JsonUtils;
import com.highway.etc.common.StationInfo;
import com.highway.etc.sink.MySqlAlertSink;
import com.highway.etc.util.DistanceUtil;

public class PlateCloneDetectionJob {

    private static final String PROPS_FILE = "application.properties";
    private static final String STATION_RESOURCE = "/stations.json";

    public static void main(String[] args) throws Exception {
        Properties props = JsonUtils.loadProperties(PROPS_FILE, "src/main/resources/" + PROPS_FILE);

        String kafkaServers = JsonUtils.requireProperty(props, "kafka.bootstrap.servers");
        String topic = JsonUtils.requireProperty(props, "kafka.topic");
        String mysqlUrl = JsonUtils.requireProperty(props, "mysql.url");
        String mysqlUser = JsonUtils.requireProperty(props, "mysql.user");
        String mysqlPwd = JsonUtils.requireProperty(props, "mysql.password");
        String alertInsertSql = JsonUtils.requireProperty(props, "mysql.alert.insert.sql");
        long watermarkOutOfOrderMs = Long.parseLong(JsonUtils.optionalProperty(props, "event.out.of.order.ms", "120000"));
        double maxSpeed = Double.parseDouble(JsonUtils.optionalProperty(props, "clone.max.speed.kmh", "160"));
        long minDiffSec = Long.parseLong(JsonUtils.optionalProperty(props, "clone.min.time.diff.sec", "10"));
        long checkpointInterval = Long.parseLong(JsonUtils.optionalProperty(props, "checkpoint.interval.ms", "60000"));
        long checkpointMinPause = Long.parseLong(JsonUtils.optionalProperty(props, "checkpoint.min.pause.ms", "30000"));
        long checkpointTimeout = Long.parseLong(JsonUtils.optionalProperty(props, "checkpoint.timeout.ms", "120000"));
        int parallelism = Integer.parseInt(JsonUtils.optionalProperty(props, "job.parallelism", "2"));

        Map<Integer, StationInfo> stationMap = loadStationMap();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointMinPause);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        env.getConfig().setAutoWatermarkInterval(2000);

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics(topic)
                .setGroupId(props.getProperty("kafka.group.id", "plate-clone-consumer"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new BasicEventDeserializer())
                .build();

        WatermarkStrategy<Event> wm = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(watermarkOutOfOrderMs))
                .withTimestampAssigner((e, ts) -> e.gcsj.toEpochMilli());

        DataStream<Event> stream = env.fromSource(source, wm, "kafka-source-clone")
                .filter(e -> e != null && e.hasEssentialFields() && e.hphm != null && !e.hphm.isBlank());

        stream
                .keyBy(e -> e.hphm)
                .process(new CloneDetectProcessFunction(stationMap, maxSpeed, minDiffSec))
                .addSink(new MySqlAlertSink(mysqlUrl, mysqlUser, mysqlPwd, alertInsertSql))
                .name("mysql-alert-sink");

        env.execute("PlateCloneDetectionJob");
    }

    static Map<Integer, StationInfo> loadStationMap() throws Exception {
        List<StationInfo> stations = JsonUtils.readListFromClasspath(STATION_RESOURCE, StationInfo.class);
        Map<Integer, StationInfo> map = new HashMap<>();
        for (StationInfo s : stations) {
            if (s != null) {
                map.put(s.stationId, s);
            }
        }
        if (map.isEmpty()) {
            throw new IllegalStateException("No station metadata loaded from " + STATION_RESOURCE);
        }
        return map;
    }

    // 反序列化：出错时返回“空事件”，上游 filter 丢弃
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
                String time = n.path("gcsj").asText(null);
                e.gcsj = time == null ? Instant.now() : Instant.parse(time);
                e.clppxh = n.path("clppxh").asText(null);
                return e;
            } catch (Exception ex) {
                Event e = new Event();
                e.stationId = 0;
                e.gcsj = Instant.now();
                return e;
            }
        }
    }

    public static class CloneDetectProcessFunction extends KeyedProcessFunction<String, Event, Alert> {

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
                    .newBuilder(Time.minutes(45))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .cleanupFullSnapshot()
                    .build();
            ListStateDescriptor<Event> desc = new ListStateDescriptor<>("plate-history", Event.class);
            desc.enableTimeToLive(ttl);
            historyState = getRuntimeContext().getListState(desc);
        }

        @Override
        public void processElement(Event current, Context ctx, Collector<Alert> out) throws Exception {
            if (current == null || current.hphm == null) {
                return;
            }

            List<Event> retained = new ArrayList<>();
            for (Event e : historyState.get()) {
                if (e != null && withinWindow(e, current)) {
                    retained.add(e);
                }
            }
            retained.add(current);
            historyState.update(retained);

            StationInfo currentStation = stationMap.get(current.stationId);
            if (currentStation == null || !currentStation.hasGeo()) {
                return;
            }

            for (Event prev : retained) {
                if (prev == current || prev.stationId == current.stationId) {
                    continue;
                }
                long diffSec = Math.abs(current.gcsj.getEpochSecond() - prev.gcsj.getEpochSecond());
                if (diffSec < minDiffSec) {
                    continue;
                }
                StationInfo prevStation = stationMap.get(prev.stationId);
                if (prevStation == null || !prevStation.hasGeo()) {
                    continue;
                }

                double distanceKm = DistanceUtil.haversine(
                        currentStation.lat, currentStation.lon,
                        prevStation.lat, prevStation.lon);
                double hours = diffSec / 3600.0;
                double speedKmh = hours == 0 ? Double.POSITIVE_INFINITY : distanceKm / hours;

                if (speedKmh > maxSpeedKmh) {
                    boolean currentAfterPrev = current.gcsj.isAfter(prev.gcsj);
                    int firstId = currentAfterPrev ? prev.stationId : current.stationId;
                    int secondId = currentAfterPrev ? current.stationId : prev.stationId;
                    int shardStationId = firstId; // route by the earlier station to satisfy MyCat sharding
                    Alert alert = Alert.of(
                            shardStationId,
                            maskPlate(current),
                            firstId,
                            secondId,
                            diffSec,
                            distanceKm,
                            speedKmh,
                            Math.min(1.0, (speedKmh - maxSpeedKmh) / maxSpeedKmh + 0.5));
                    out.collect(alert);
                }
            }
        }

        private boolean withinWindow(Event candidate, Event current) {
            long diffSec = Math.abs(current.gcsj.getEpochSecond() - candidate.gcsj.getEpochSecond());
            return diffSec <= Time.minutes(45).toMilliseconds() / 1000;
        }

        private String maskPlate(Event event) {
            if (event.hphmMask != null && !event.hphmMask.isBlank()) {
                return event.hphmMask;
            }
            if (event.hphm == null || event.hphm.isBlank()) {
                return "UNKNOWN";
            }
            int visible = Math.min(4, event.hphm.length());
            return event.hphm.substring(0, visible) + "****";
        }
    }
}
