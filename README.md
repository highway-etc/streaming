# streaming (Flink)

- src/: Flink 作业（预处理/聚合/套牌检测规则版）
- build: Dockerfile
- .github/workflows/java-ci.yml: CI 构建 + 推送镜像（ghcr）

## 本地构建

```powershell
mvn -B -DskipTests package
```

- 产物：`target/streaming-0.1.0.jar`（版本号以 pom 为准）。
- 将 JAR 拷贝到 infra/flink/usrlib 后，可被 docker-compose 挂载到 Flink 容器 `/opt/flink/usrlib`。

## 本地运行（示例）

- 连接 infra 提供的 Kafka/MySQL/MyCat（默认网络名 infra_etcnet）。
- 在 Flink UI 上传 JAR 并指定入口类：
 	- 明细+窗口统计：`com.highway.etc.job.TrafficStreamingJob`
 	- 套牌告警：`com.highway.etc.job.PlateCloneDetectionJob`
- 或通过 CLI：

```powershell
docker exec -it flink-jobmanager /opt/flink/bin/flink run -d \
 -c com.highway.etc.job.TrafficStreamingJob \
 /opt/flink/usrlib/streaming-0.1.0.jar
```

## 外部依赖

- Kafka 主题：`etc_traffic`（producer 可用 infra/scripts/send_mock_data.ps1）。
- MyCat/MySQL：逻辑库 `highway_etc`，分库分表规则与 infra 仓库保持一致。
- 额外依赖 JAR 放在 `infra/flink/extra-lib`，compose 启动时会挂载到 `/opt/flink/extra-lib`。
