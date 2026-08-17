package com.kafkamart.analytics;

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
        java.time.Instant deadline = java.time.Instant.now().plusSeconds(45);
        int status = 0;
        while (java.time.Instant.now().isBefore(deadline)) {
            status = given().when().get("/q/health/ready").then().extract().statusCode();
            if (status == 200) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        }
        throw new AssertionError("readiness not UP, last status=" + status);
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
