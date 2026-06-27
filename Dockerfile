ARG MAVEN_IMAGE=maven:3.9-eclipse-temurin-17
ARG RUNTIME_IMAGE=eclipse-temurin:17-jre

FROM ${MAVEN_IMAGE} AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn package -DskipTests -q

FROM ${RUNTIME_IMAGE}
WORKDIR /app
COPY --from=builder /app/target/monitor-be-0.0.1-SNAPSHOT.jar app.jar
ENV TZ=Asia/Shanghai
ENV JAVA_OPTS="-Xmx512m -Xms256m -XX:+UseSerialGC"
EXPOSE 8200
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
