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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

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

        DataStream<Event> raw = env.fromSource(
                kafkaSource,
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(java.time.Duration.ofMillis(watermarkOutOfOrderMs))
                        .withTimestampAssigner((e, ts) -> e.gcsj.toEpochMilli()),
                "kafka-source"
        );

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

        // 按站点滚动窗口 30s 统计
        DataStream<StatsRecord> stats = enriched
                .keyBy((KeySelector<EnrichedEvent, Integer>) e -> e.stationId)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .process(new ProcessWindowFunction<EnrichedEvent, StatsRecord, Integer, TimeWindow>() {
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
                }).name("window-agg-stats");

        stats.addSink(new MySqlStatsSink(mysqlUrl, mysqlUser, mysqlPwd, statsInsertSql))
                .name("mysql-stats-sink");

        env.execute("TrafficStreamingJob");
    }

    // 自定义反序列化：Flink 1.18 的 AbstractDeserializationSchema#deserialize 不再声明 throws Exception
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
                    e.gcsj = Instant.parse(gcsjStr);
                } catch (DateTimeParseException ex) {
                    e.gcsj = Instant.now();
                }
                e.hpzl = n.path("hpzl").asText(null);
                e.hphm = n.path("hphm").asText(null);
                e.hphmMask = n.path("hphm_mask").asText(null);
                e.clppxh = n.path("clppxh").asText(null);
                return e;
            } catch (Exception ex) {
                // 解析失败时返回一个默认事件或丢弃（可按需改为 null 并在上游 filter）
                Event e = new Event();
                e.gcsj = Instant.now();
                return e;
            }
        }
    }
}