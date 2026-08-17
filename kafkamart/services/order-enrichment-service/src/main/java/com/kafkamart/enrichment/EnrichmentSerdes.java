package com.kafkamart.enrichment;

import com.kafkamart.avro.UserProfile;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;

final class EnrichmentSerdes {
    private EnrichmentSerdes() {}

    static Map<String, Object> schemaRegistryConfig(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        config.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return config;
    }

    static Serde<UserProfile> userProfile(String schemaRegistryUrl) {
        SpecificAvroSerde<UserProfile> serde = new SpecificAvroSerde<>();
        serde.configure(schemaRegistryConfig(schemaRegistryUrl), false);
        return serde;
    }
}
