FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn package -DskipTests -q

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        fontconfig \
        fonts-liberation \
        fonts-noto-cjk \
        gnupg \
        libasound2t64 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libcairo2 \
        libcups2 \
        libdbus-1-3 \
        libdrm2 \
        libgbm1 \
        libglib2.0-0 \
        libgtk-3-0 \
        libnspr4 \
        libnss3 \
        libpango-1.0-0 \
        libx11-6 \
        libxcb1 \
        libxcomposite1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxrandr2 \
        xdg-utils \
    && install -d -m 0755 /etc/apt/keyrings \
    && curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/keyrings/google-linux.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && fc-cache -f \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/monitor-be-0.0.1-SNAPSHOT.jar app.jar
COPY docker/playwright/stealth.min.js /opt/youbi/playwright/stealth.min.js
ENV TZ=Asia/Shanghai
ENV YDBI_XHS_BROWSER_CHANNEL=chrome
ENV YDBI_DOUYIN_STEALTH_SCRIPT_PATH=/opt/youbi/playwright/stealth.min.js
ENV YDBI_XHS_STEALTH_SCRIPT_PATH=/opt/youbi/playwright/stealth.min.js
ENV YDBI_BILIBILI_PLAYWRIGHT_STEALTH_SCRIPT_PATH=/opt/youbi/playwright/stealth.min.js
ENV YDBI_SHIPINHAO_HEADLESS=true
ENV YDBI_SHIPINHAO_STEALTH_SCRIPT_PATH=/opt/youbi/playwright/stealth.min.js
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
ENV JAVA_OPTS="-Xmx512m -Xms256m -XX:+UseSerialGC"
EXPOSE 8200
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
