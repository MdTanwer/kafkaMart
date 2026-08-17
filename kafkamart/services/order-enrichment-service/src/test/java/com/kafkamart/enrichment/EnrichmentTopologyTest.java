package com.kafkamart.enrichment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kafkamart.avro.UserProfile;
import com.kafkamart.common.Topics;
import com.kafkamart.common.event.EnrichedOrder;
import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.event.OrderItem;
import com.kafkamart.common.serde.JsonSerde;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EnrichmentTopologyTest {
    static final String MOCK_SR = "mock://enrichment-ttd";

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderCreated> orders;
    private TestInputTopic<String, UserProfile> users;
    private TestOutputTopic<String, EnrichedOrder> enriched;
    private Serde<OrderCreated> orderSerde;
    private Serde<EnrichedOrder> enrichedSerde;
    private Serde<UserProfile> userSerde;

    @BeforeEach
    void startDriver() throws Exception {
        orderSerde = JsonSerde.of(OrderCreated.class);
        enrichedSerde = JsonSerde.of(EnrichedOrder.class);
        userSerde = EnrichmentSerdes.userProfile(MOCK_SR);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrichment-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory("enrichment-topology-test").toString());
        driver = new TopologyTestDriver(EnrichmentTopology.build(MOCK_SR), props);
        orders =
                driver.createInputTopic(
                        Topics.ORDERS, Serdes.String().serializer(), orderSerde.serializer());
        users =
                driver.createInputTopic(
                        Topics.USERS, Serdes.String().serializer(), userSerde.serializer());
        enriched =
                driver.createOutputTopic(
                        Topics.ORDERS_ENRICHED,
                        Serdes.String().deserializer(),
                        enrichedSerde.deserializer());
    }

    @AfterEach
    void closeDriver() {
        if (driver != null) {
            driver.close();
        }
        if (orderSerde != null) {
            orderSerde.close();
        }
        if (enrichedSerde != null) {
            enrichedSerde.close();
        }
        if (userSerde != null) {
            userSerde.close();
        }
    }

    @Test
    void orderJoinsWhenProfileIsAlreadyInTheTable() {
        users.pipeInput("ada", profile("ada", "Ada Lovelace", "ada@kafkamart.dev"));
        OrderCreated order = order("ord-1", "ada");
        orders.pipeInput("ada", order);

        List<TestRecord<String, EnrichedOrder>> out = enriched.readRecordsToList();
        assertEquals(1, out.size());
        assertEquals("ord-1", out.get(0).getKey());
        EnrichedOrder value = out.get(0).getValue();
        assertEquals("ada", value.userId());
        assertEquals("Ada Lovelace", value.userName());
        assertEquals("ada@kafkamart.dev", value.userEmail());
        assertEquals(order.orderId(), value.orderId());

        KeyValueStore<String, EnrichedOrder> store =
                driver.getKeyValueStore(EnrichmentTopology.ORDERS_STORE);
        assertEquals("Ada Lovelace", store.get("ord-1").userName());
    }

    @Test
    void orderBeforeProfileStaysUnenrichedAndIsNotReplayed() {
        // Inner KStream-KTable join: the stream record is looked up against the current table.
        // If the user is missing, the order is dropped. A later profile update does NOT
        // reprocess that order (the table changelog does not rewind the stream).
        OrderCreated early = order("ord-early", "ada");
        orders.pipeInput("ada", early);
        assertTrue(enriched.isEmpty(), "order before profile must not emit EnrichedOrder");

        users.pipeInput("ada", profile("ada", "Ada Lovelace", "ada@kafkamart.dev"));
        assertTrue(
                enriched.isEmpty(),
                "profile arriving later must not retroactively enrich the dropped order");

        KeyValueStore<String, EnrichedOrder> store =
                driver.getKeyValueStore(EnrichmentTopology.ORDERS_STORE);
        assertNull(store.get("ord-early"));

        orders.pipeInput("ada", order("ord-late", "ada"));
        List<TestRecord<String, EnrichedOrder>> out = enriched.readRecordsToList();
        assertEquals(1, out.size());
        assertEquals("ord-late", out.get(0).getValue().orderId());
        assertEquals("Ada Lovelace", out.get(0).getValue().userName());
    }

    @Test
    void livingTableNewOrderSeesUpdatedProfile() {
        users.pipeInput("ada", profile("ada", "Ada Lovelace", "ada@kafkamart.dev"));
        orders.pipeInput("ada", order("ord-old", "ada"));
        assertEquals("Ada Lovelace", enriched.readRecord().getValue().userName());

        users.pipeInput("ada", profile("ada", "Ada Lovelace II", "ada@kafkamart.dev"));
        orders.pipeInput("ada", order("ord-new", "ada"));
        assertEquals("Ada Lovelace II", enriched.readRecord().getValue().userName());
    }

    @Test
    void globalKTableJoinDoesNotNeedCopartitioning() throws Exception {
        driver.close();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrichment-global-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory("enrichment-global-test").toString());
        driver =
                new TopologyTestDriver(
                        EnrichmentTopology.build(MOCK_SR, 3, true, () -> {}, () -> {}), props);
        orders =
                driver.createInputTopic(
                        Topics.ORDERS, Serdes.String().serializer(), orderSerde.serializer());
        users =
                driver.createInputTopic(
                        Topics.USERS, Serdes.String().serializer(), userSerde.serializer());
        enriched =
                driver.createOutputTopic(
                        Topics.ORDERS_ENRICHED,
                        Serdes.String().deserializer(),
                        enrichedSerde.deserializer());

        users.pipeInput("ada", profile("ada", "Ada Global", "ada@kafkamart.dev"));
        orders.pipeInput("ignored-key", order("ord-g", "ada"));
        EnrichedOrder value = enriched.readRecord().getValue();
        assertEquals("Ada Global", value.userName());
        assertEquals("ord-g", value.orderId());
    }

    private static UserProfile profile(String userId, String name, String email) {
        return UserProfile.newBuilder().setUserId(userId).setName(name).setEmail(email).build();
    }

    private static OrderCreated order(String orderId, String userId) {
        return new OrderCreated(
                UUID.randomUUID(),
                java.time.Instant.parse("2026-01-01T00:00:00Z"),
                "trace-" + orderId,
                orderId,
                userId,
                List.of(new OrderItem("SKU-1", 1, new BigDecimal("9.99"))),
                new BigDecimal("9.99"),
                "idem-" + orderId,
                "USD");
    }
}
