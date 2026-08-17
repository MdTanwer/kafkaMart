package com.kafkamart.inventory;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;

@QuarkusTest
class HealthTest {
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
        given().when().get("/q/metrics").then()
                .statusCode(200)
                .body(containsString("kafkamart_events_produced"));
    }

    @Test
    void openApiAvailable() {
        given().when().get("/q/openapi").then().statusCode(200);
    }
}
