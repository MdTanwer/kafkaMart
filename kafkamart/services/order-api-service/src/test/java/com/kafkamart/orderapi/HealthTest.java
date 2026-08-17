package com.kafkamart.orderapi;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class HealthTest {
    @Inject ServiceMetrics metrics;

    @Test
    void livenessUp() {
        given().when().get("/q/health/live").then().statusCode(200);
    }

    @Test
    void readinessUpWhenBrokerAvailable() {
        given().when().get("/q/health/ready").then().statusCode(200);
    }

    @Test
    void metricsExposeBusinessCounters() {
        metrics.produced();
        given().when()
                .get("/q/metrics")
                .then()
                .statusCode(200)
                .body(containsString("kafkamart_events_produced"));
    }

    @Test
    void openApiAvailable() {
        given().when().get("/q/openapi").then().statusCode(200);
    }
}
