FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY target/ops-monitor-service-1.0.0-SNAPSHOT.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java","-XX:+UseG1GC","-jar","/app/app.jar"]
