package com.highway.etc.job;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.highway.etc.common.EnrichedEvent;
import com.highway.etc.common.Event;
import com.highway.etc.common.JsonUtils;
import com.highway.etc.common.StatsRecord;
import com.highway.etc.sink.MySqlBatchSink;
import com.highway.etc.sink.MySqlStatsSink;

public class TrafficStreamingJob {

    private static final String PROPS_FILE = "application.properties";

    public static void main(String[] args) throws Exception {
        Properties props = JsonUtils.loadProperties(PROPS_FILE, "src/main/resources/" + PROPS_FILE);

        String kafkaServers = JsonUtils.requireProperty(props, "kafka.bootstrap.servers");
        String topic = JsonUtils.requireProperty(props, "kafka.topic");
        String mysqlUrl = JsonUtils.requireProperty(props, "mysql.url");
        String mysqlUser = JsonUtils.requireProperty(props, "mysql.user");
        String mysqlPwd = JsonUtils.requireProperty(props, "mysql.password");
        int batchSize = Integer.parseInt(JsonUtils.optionalProperty(props, "mysql.batch.size", "500"));
        String insertSql = JsonUtils.requireProperty(props, "mysql.insert.sql");
        String statsInsertSql = JsonUtils.requireProperty(props, "mysql.stats.insert.sql");
        long watermarkOutOfOrderMs = Long.parseLong(JsonUtils.optionalProperty(props, "event.out.of.order.ms", "120000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(2000);

        KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics(topic)
                .setGroupId(props.getProperty("kafka.group.id", "traffic-consumer"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new EventDeserializer())
                .build();

        WatermarkStrategy<Event> wm = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(watermarkOutOfOrderMs))
                .withTimestampAssigner((e, ts) -> e.gcsj.toEpochMilli());

        DataStream<Event> raw = env.fromSource(kafkaSource, wm, "kafka-source")
                .filter(e -> e != null && e.hasEssentialFields());

        DataStream<EnrichedEvent> enriched = raw.map(TrafficStreamingJob::enrich).name("enrich-map");

        enriched.addSink(new MySqlBatchSink(mysqlUrl, mysqlUser, mysqlPwd, insertSql, batchSize))
                .name("mysql-batch-sink");

        DataStream<StatsRecord> stats = enriched
                .keyBy((KeySelector<EnrichedEvent, Integer>) e -> e.stationId)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new StatsWindowFn())
                .name("window-agg-stats");

        stats.addSink(new MySqlStatsSink(mysqlUrl, mysqlUser, mysqlPwd, statsInsertSql))
                .name("mysql-stats-sink");

        env.execute("TrafficStreamingJob");
    }

    private static EnrichedEvent enrich(Event e) {
        EnrichedEvent ne = EnrichedEvent.from(e);
        ne.xzqhmc = e.xzqhmc == null ? "" : e.xzqhmc;
        ne.kkmc = e.kkmc == null ? "" : e.kkmc;
        ne.fxlx = e.fxlx == null ? "" : e.fxlx;
        ne.hpzl = e.hpzl == null ? "" : e.hpzl;
        ne.hphm = e.hphm == null ? "" : e.hphm;
        ne.hphmMask = maskPlate(e);
        ne.clppxh = e.clppxh == null ? "" : e.clppxh;
        ne.tags = Collections.emptyMap();
        return ne;
    }

    private static String maskPlate(Event event) {
        if (event.hphmMask != null && !event.hphmMask.isBlank()) {
            return event.hphmMask;
        }
        if (event.hphm == null || event.hphm.isEmpty()) {
            return "UNKNOWN";
        }
        int visible = Math.min(4, event.hphm.length());
        return event.hphm.substring(0, visible) + "****";
    }

    private static class StatsWindowFn extends ProcessWindowFunction<EnrichedEvent, StatsRecord, Integer, TimeWindow> {

        @Override
        public void process(Integer key, Context context, Iterable<EnrichedEvent> elements, Collector<StatsRecord> out) {
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
    }

    // Flink 1.18: 反序列化不要返回 null
    public static class EventDeserializer extends AbstractDeserializationSchema<Event> {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Event deserialize(byte[] message) {
            try {
                JsonNode n = mapper.readTree(message);
                Event e = new Event();
                e.gcxh = n.path("gcxh").asLong();
                e.xzqhmc = n.path("xzqhmc").asText(null);
                e.adcode = n.path("adcode").asInt(0);
                e.kkmc = n.path("kkmc").asText(null);
                e.stationId = n.path("station_id").asInt(0);
                e.fxlx = n.path("fxlx").asText(null);
                String gcsjStr = n.path("gcsj").asText(null);
                try {
                    e.gcsj = gcsjStr == null ? Instant.now() : Instant.parse(gcsjStr);
                } catch (Exception ignore) {
                    e.gcsj = Instant.now();
                }
                e.hpzl = n.path("hpzl").asText(null);
                e.hphm = n.path("hphm").asText(null);
                e.hphmMask = n.path("hphm_mask").asText(null);
                e.clppxh = n.path("clppxh").asText(null);
                return e;
            } catch (Exception ex) {
                Event e = new Event();
                e.gcsj = Instant.now();
                e.stationId = 0; // 让上游 filter 掉
                return e;
            }
        }
    }
}
