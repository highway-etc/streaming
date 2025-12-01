FROM eclipse-temurin:17-jre
WORKDIR /app
# 构建后复制 (假设使用 gradle shadowJar 或单 jar)
COPY build/libs/streaming-0.1.0.jar /app/app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]