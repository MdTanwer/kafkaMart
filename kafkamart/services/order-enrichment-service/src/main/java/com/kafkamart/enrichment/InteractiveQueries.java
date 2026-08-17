package com.kafkamart.enrichment;

import com.kafkamart.common.event.EnrichedOrder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interactive Queries facade over {@code orders-store}.
 *
 * <p>Quarkus 3.33 does not ship {@code io.quarkus.kafka.streams.runtime.InteractiveQueries}; the
 * runtime CDI bean is {@link KafkaStreams}. This class is the IQ layer the REST resource injects.
 */
@ApplicationScoped
public class InteractiveQueries {
    private static final Logger LOG = LoggerFactory.getLogger(InteractiveQueries.class);

    @Inject KafkaStreams streams;

    @ConfigProperty(name = "quarkus.kafka-streams.application-server")
    String applicationServer;

    public OrderQueryResult find(String orderId) {
        HostInfo local = HostInfo.buildFromEndpoint(applicationServer);
        KeyQueryMetadata metadata =
                streams.queryMetadataForKey(
                        EnrichmentTopology.ORDERS_STORE, orderId, Serdes.String().serializer());
        if (metadata == null || metadata == KeyQueryMetadata.NOT_AVAILABLE) {
            return OrderQueryResult.miss(
                    orderId, local.host(), local.port(), "metadata unavailable");
        }
        HostInfo active = metadata.activeHost();
        if (!isLocal(local, active)) {
            LOG.info(
                    "IQ key={} hosted on {}:{} (this instance {}:{})",
                    orderId,
                    active.host(),
                    active.port(),
                    local.host(),
                    local.port());
            return OrderQueryResult.miss(
                    orderId, active.host(), active.port(), "not local — route to metadata.host");
        }
        EnrichedOrder found = store().get(orderId);
        if (found == null) {
            return OrderQueryResult.miss(orderId, local.host(), local.port(), "not found");
        }
        return OrderQueryResult.hit(found, local.host(), local.port());
    }

    private ReadOnlyKeyValueStore<String, EnrichedOrder> store() {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        InvalidStateStoreException last = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                return streams.store(
                        StoreQueryParameters.fromNameAndType(
                                EnrichmentTopology.ORDERS_STORE,
                                QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException notReady) {
                last = notReady;
                try {
                    Thread.sleep(50);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw notReady;
                }
            }
        }
        throw last == null ? new InvalidStateStoreException("orders-store not ready") : last;
    }

    static boolean isLocal(HostInfo local, HostInfo active) {
        if (active == null) {
            return false;
        }
        return local.host().equals(active.host()) && local.port() == active.port();
    }
}
