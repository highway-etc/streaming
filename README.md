# streaming (Flink)

- src/: Flink 作业（预处理/聚合/套牌检测规则版）
- build: Dockerfile
- .github/workflows/java-ci.yml: CI 构建 + 推送镜像（ghcr）

## 本地运行（示例）

- 连接 infra 提供的 Kafka/MySQL
- 打包并在 Flink UI 提交 Jar，或用 Flink CLI 提交
