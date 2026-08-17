package com.kafkamart.userprofile;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkamart.common.test.AbstractKafkaDevServiceTest;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@QuarkusTest
class UserProfileTest extends AbstractKafkaDevServiceTest {
    @Inject UserProfileCache cache;
    @Inject UserWireCapture wire;

    @Test
    void postThenGetServesLatestFromCompactedCache() {
        String userId = "ada-" + UUID.randomUUID();
        postUser(userId, "Ada Lovelace", "ada@kafkamart.dev");
        awaitCached(userId, "Ada Lovelace");
        given().when()
                .get("/api/users/{id}", userId)
                .then()
                .statusCode(200)
                .body("userId", equalTo(userId))
                .body("name", equalTo("Ada Lovelace"))
                .body("email", equalTo("ada@kafkamart.dev"));

        postUser(userId, "Ada Lovelace II", "ada@kafkamart.dev");
        awaitCached(userId, "Ada Lovelace II");
        given().when()
                .get("/api/users/{id}", userId)
                .then()
                .statusCode(200)
                .body("name", equalTo("Ada Lovelace II"));
    }

    @Test
    void avroWireFormatHasMagicByteAndSchemaId() {
        String userId = "wire-" + UUID.randomUUID();
        postUser(userId, "Wire User", "wire@kafkamart.dev");
        UserWireCapture.Captured captured = awaitWire(userId);
        assertNotNull(captured.value());
        assertTrue(
                captured.value().length >= 5, "Confluent Avro payload is magic + schema id + body");
        assertEquals(0x00, captured.value()[0] & 0xFF, "magic byte must be 0x00");
        int schemaId = ByteBuffer.wrap(captured.value(), 1, 4).getInt();
        assertTrue(schemaId > 0, "schema id should be a positive registry id, was " + schemaId);
    }

    @Test
    void unknownUserIs404() {
        given().when().get("/api/users/no-such-user").then().statusCode(404);
    }

    private static void postUser(String userId, String name, String email) {
        given().contentType(ContentType.JSON)
                .body(
                        "{\"userId\":\"%s\",\"name\":\"%s\",\"email\":\"%s\"}"
                                .formatted(userId, name, email))
                .when()
                .post("/api/users")
                .then()
                .statusCode(202)
                .body("userId", equalTo(userId));
    }

    private void awaitCached(String userId, String expectedName) {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            var hit = cache.get(userId);
            if (hit.isPresent() && expectedName.equals(String.valueOf(hit.get().getName()))) {
                return;
            }
            sleep(50);
        }
        throw new AssertionError("cache did not reach userId=" + userId + " name=" + expectedName);
    }

    private UserWireCapture.Captured awaitWire(String userId) {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            for (UserWireCapture.Captured captured : wire.snapshot()) {
                if (userId.equals(captured.key()) && captured.value() != null) {
                    return captured;
                }
            }
            sleep(50);
        }
        throw new AssertionError("no raw users record for key=" + userId);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted");
        }
    }
}
