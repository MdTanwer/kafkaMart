FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY target/order-api-service-1.0.0-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java","-XX:+UseG1GC","-jar","/app/app.jar"]
