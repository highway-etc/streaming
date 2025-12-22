# streaming：Flink 实时处理作业

实时消费 Kafka 车流数据，做清洗、去重、窗口聚合并写入 MyCat/MySQL；同时进行套牌告警检测。包含两条主作业：TrafficStreamingJob 与 PlateCloneDetectionJob。

## 技术栈
- Flink 1.18（Scala 2.12，Java 17）
- Kafka Source，MySQL/MyCat Sink（批量写/单条写）
- 自定义反序列化、状态 TTL 去重、30 秒滚动窗口聚合

## 目录速览
- src/main/java/com/highway/etc/job/TrafficStreamingJob.java：明细清洗去重批量落库30s 窗口统计
- src/main/java/com/highway/etc/job/PlateCloneDetectionJob.java：按车牌分区，基于经纬度/时间差计算速度做套牌告警
- src/main/resources/application.properties：运行参数（kafka.bootstrap.servers、mysql.url/user/password、checkpoint 等）
- sql/schema.sql：表结构参考
- Dockerfile：用于打包镜像（可选）

## 构建与产物
```powershell
mvn -B -DskipTests package
```
- 生成 target/streaming-0.1.0.jar（版本号以 pom 为准）
- 将 JAR 拷贝到 infra/flink/usrlib，docker-compose 会挂载到 /opt/flink/usrlib

## 提交作业（连接 infra 环境）
```powershell
# 明细+窗口统计
docker exec -it flink-jobmanager /opt/flink/bin/flink run -d \
  -c com.highway.etc.job.TrafficStreamingJob \
  /opt/flink/usrlib/streaming-0.1.0.jar

# 套牌告警
docker exec -it flink-jobmanager /opt/flink/bin/flink run -d \
  -c com.highway.etc.job.PlateCloneDetectionJob \
  /opt/flink/usrlib/streaming-0.1.0.jar
```
- 请保证 application.properties 指向 kafka:9092、mycat:8066（在 infra_etcnet 网络内）。
- 默认并行度在配置中可调，确保不超过 TaskManager 槽位。

## 运行参数要点（application.properties）
- kafka.bootstrap.servers：Kafka 地址，默认 kafka:9092
- kafka.topic：etc_traffic
- mysql.url/user/password：指向 MyCat 或直连 MySQL 分片
- mysql.insert.sql：traffic_pass_dev 的 insert 语句（批量 sink 使用）
- mysql.stats.insert.sql：stats_realtime 的 insert 语句
- event.out.of.order.ms：水位延迟，默认 120000 ms
- dedup.hours：去重 TTL，默认 24 小时
- checkpoint.interval.ms / checkpoint.timeout.ms / checkpoint.min.pause.ms：容错参数
- clone.max.speed.kmh / clone.min.time.diff.sec：套牌检测阈值

## 数据流说明
1) Kafka JSON 事件（字段见 common/Event.java）被 TrafficStreamingJob 清洗，缺失字段填空，车牌做脱敏 mask。
2) 去重键：gcxh 优先，否则 plate+station+秒级桶；TTL 控制重复时间窗。
3) 批量 sink：按配置批大小写入 traffic_pass_dev。
4) 30 秒滚动窗口聚合，输出 stats_realtime（含方向分布 by_dir、车型分布 by_type）。
5) PlateCloneDetectionJob 维护 45 分钟车牌轨迹，计算跨站速度，超阈值则写入 alert 表。

## 监控与排查
- 查看作业：`docker exec -it flink-jobmanager /opt/flink/bin/flink list`
- 取消作业：`flink cancel <jobId>`
- 日志：`docker exec -it flink-jobmanager tail -n 200 /opt/flink/log/flink--jobmanager-0.log`
- 指标：Prometheus 端口 9250-9260（compose 已挂载 metrics reporter），可在 Grafana 导入 Flink Dashboard 12019。

## 常见问题
- Kafka 反序列化报错：检查事件 JSON 字段名是否匹配 common/Event 定义；反序列化失败会产生日志并丢弃。
- JDBC 写入失败：确认 MyCat 已启动且账号 etcuser/etcpass 有权限；检查 insert SQL 与表结构字段匹配。
- 水位/延迟过高：调整 event.out.of.order.ms、checkpoint 周期，并检查 Kafka 积压。
