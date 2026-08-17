package com.kafkamart.analytics;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

@QuarkusTest
class AnalyticsResourceTest {
    @Test
    void upsertUserIsQueryableAndCdcBufferStartsEmpty() {
        given().when().get("/api/analytics/cdc").then().statusCode(200).body("", hasSize(0));

        given().contentType(ContentType.JSON)
                .body(
                        "{\"userId\":\"ada\",\"name\":\"Ada Lovelace\",\"email\":\"ada@kafkamart.dev\"}")
                .when()
                .post("/api/analytics/cdc/users")
                .then()
                .statusCode(200)
                .body("userId", equalTo("ada"))
                .body("name", equalTo("Ada Lovelace"));

        given().when().get("/api/analytics/orders").then().statusCode(200);
    }

    @Test
    void connectorsEndpointSurvivesMissingConnectCluster() {
        int status = given().when().get("/api/analytics/connectors").then().extract().statusCode();
        assert status == 200 || status == 502;
    }
}
