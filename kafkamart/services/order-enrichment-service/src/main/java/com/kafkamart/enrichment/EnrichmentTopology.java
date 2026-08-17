package com.kafkamart.enrichment;

import com.kafkamart.avro.UserProfile;
import com.kafkamart.common.Topics;
import com.kafkamart.common.event.EnrichedOrder;
import com.kafkamart.common.event.OrderCreated;
import com.kafkamart.common.serde.JsonSerde;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KStream-KTable join of {@code orders} (JSON) with compacted {@code users} (Avro).
 *
 * <p>Default join is <b>inner</b>: an order whose {@code userId} is not yet in the KTable is
 * dropped and is <b>not</b> replayed when the profile arrives later. Place the profile first (or
 * send a new order after the table updates).
 *
 * <p>{@code USE_GLOBAL_KTABLE}: commented toggle — see {@link #joinGlobalKTable}.
 */
@ApplicationScoped
public class EnrichmentTopology {
    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentTopology.class);

    static final String ORDERS_STORE = "orders-store";
    static final String USERS_STORE = "users-store";
    static final String USERS_GLOBAL_STORE = "users-global-store";
    static final String REPARTITION_NAME = "orders-by-user";
    static final String JOIN_MODE_GLOBAL = "global";

    /**
     * Commented toggle (also {@code enrichment.join.mode=global}): GlobalKTable broadcasts the full
     * users table to every instance — no copartitioning with {@code orders}.
     */
    static final boolean USE_GLOBAL_KTABLE = false;

    @Inject ServiceMetrics metrics;

    @ConfigProperty(name = "kafka-streams.schema.registry.url")
    Optional<String> streamsRegistry;

    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.schema.registry.url")
    Optional<String> messagingRegistry;

    @ConfigProperty(name = "schema.registry.url")
    Optional<String> plainRegistry;

    @ConfigProperty(name = "enrichment.users.partitions", defaultValue = "3")
    int usersPartitions;

    @ConfigProperty(name = "enrichment.join.mode", defaultValue = "ktable")
    String joinMode;

    @Produces
    public Topology topology() {
        boolean global = USE_GLOBAL_KTABLE || JOIN_MODE_GLOBAL.equalsIgnoreCase(joinMode);
        return build(
                schemaRegistryUrl(), usersPartitions, global, metrics::consumed, metrics::produced);
    }

    static Topology build(String schemaRegistryUrl) {
        return build(schemaRegistryUrl, 3, false, () -> {}, () -> {});
    }

    static Topology build(
            String schemaRegistryUrl,
            int usersPartitions,
            boolean globalKTable,
            Runnable onOrder,
            Runnable onEnriched) {
        Serde<String> stringSerde = Serdes.String();
        Serde<OrderCreated> orderSerde = JsonSerde.of(OrderCreated.class);
        Serde<EnrichedOrder> enrichedSerde = JsonSerde.of(EnrichedOrder.class);
        Serde<UserProfile> userSerde = EnrichmentSerdes.userProfile(schemaRegistryUrl);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, OrderCreated> orders =
                builder.stream(
                                Topics.ORDERS,
                                Consumed.with(stringSerde, orderSerde).withName("orders-source"))
                        .filter(
                                (key, order) -> order != null && order.userId() != null,
                                Named.as("drop-null-orders"))
                        .peek((key, order) -> onOrder.run());

        KStream<String, EnrichedOrder> joined;
        if (globalKTable) {
            GlobalKTable<String, UserProfile> users =
                    builder.globalTable(
                            Topics.USERS,
                            Consumed.with(stringSerde, userSerde).withName("users-global-source"),
                            Materialized.<String, UserProfile, KeyValueStore<Bytes, byte[]>>as(
                                            USERS_GLOBAL_STORE)
                                    .withKeySerde(stringSerde)
                                    .withValueSerde(userSerde));
            joined = joinGlobalKTable(orders, users);
        } else {
            KTable<String, UserProfile> users =
                    builder.table(
                            Topics.USERS,
                            Consumed.with(stringSerde, userSerde).withName("users-source"),
                            Materialized.<String, UserProfile, KeyValueStore<Bytes, byte[]>>as(
                                            USERS_STORE)
                                    .withKeySerde(stringSerde)
                                    .withValueSerde(userSerde));
            joined = joinKTable(orders, users, stringSerde, orderSerde, userSerde, usersPartitions);
        }

        KStream<String, EnrichedOrder> byOrderId =
                joined.peek(
                                (userId, enriched) -> {
                                    onEnriched.run();
                                    LOG.info(
                                            "ENRICHED orderId={} userId={} userName={}",
                                            enriched.orderId(),
                                            enriched.userId(),
                                            enriched.userName());
                                })
                        .selectKey(
                                (userId, enriched) -> enriched.orderId(),
                                Named.as("select-order-id"));

        byOrderId.toTable(
                Named.as("orders-table"),
                Materialized.<String, EnrichedOrder, KeyValueStore<Bytes, byte[]>>as(ORDERS_STORE)
                        .withKeySerde(stringSerde)
                        .withValueSerde(enrichedSerde));
        byOrderId.to(
                Topics.ORDERS_ENRICHED,
                Produced.with(stringSerde, enrichedSerde).withName("enriched-sink"));
        return builder.build();
    }

    /**
     * Key-based KStream-KTable join. Both sides keyed by {@code userId}. Kafka Streams still
     * requires <b>equal partition counts</b> (copartitioning). {@code orders} has 6 partitions and
     * {@code users} has 3, so we repartition the stream to {@code usersPartitions} before joining.
     * A non-key join (stream keyed by orderId) would {@code selectKey(userId)} and inherit 6
     * partitions from {@code orders} — the copartition check then fails against {@code users}(3).
     */
    static KStream<String, EnrichedOrder> joinKTable(
            KStream<String, OrderCreated> orders,
            KTable<String, UserProfile> users,
            Serde<String> stringSerde,
            Serde<OrderCreated> orderSerde,
            Serde<UserProfile> userSerde,
            int usersPartitions) {
        KStream<String, OrderCreated> byUser =
                orders.selectKey((key, order) -> order.userId(), Named.as("select-user-id"))
                        .repartition(
                                Repartitioned.<String, OrderCreated>as(REPARTITION_NAME)
                                        .withKeySerde(stringSerde)
                                        .withValueSerde(orderSerde)
                                        .withNumberOfPartitions(usersPartitions));
        return byUser.join(
                users,
                EnrichmentTopology::enrich,
                Joined.<String, OrderCreated, UserProfile>as("order-user-join")
                        .withKeySerde(stringSerde)
                        .withValueSerde(orderSerde)
                        .withOtherValueSerde(userSerde));
    }

    /**
     * GlobalKTable variant (toggle {@link #USE_GLOBAL_KTABLE} or {@code
     * enrichment.join.mode=global}).
     *
     * <p>Right choice when: the join key is not the stream key (no copartitioning possible without
     * a repartition topic), or partition counts cannot be aligned. Cost: every user update is
     * <b>broadcast</b> to every instance (full table in each RocksDB).
     */
    static KStream<String, EnrichedOrder> joinGlobalKTable(
            KStream<String, OrderCreated> orders, GlobalKTable<String, UserProfile> users) {
        return orders.join(
                users,
                (orderKey, order) -> order.userId(),
                EnrichmentTopology::enrich,
                Named.as("order-user-global-join"));
    }

    static EnrichedOrder enrich(OrderCreated order, UserProfile profile) {
        return EnrichedOrder.from(
                order, String.valueOf(profile.getName()), String.valueOf(profile.getEmail()));
    }

    private String schemaRegistryUrl() {
        return streamsRegistry
                .or(() -> messagingRegistry)
                .or(() -> plainRegistry)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "schema.registry.url is required for UserProfile Avro serde"));
    }
}
