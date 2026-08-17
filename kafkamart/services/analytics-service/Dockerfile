FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY target/analytics-service-1.0.0-SNAPSHOT.jar app.jar
EXPOSE 8088
ENTRYPOINT ["java","-XX:+UseG1GC","-jar","/app/app.jar"]
