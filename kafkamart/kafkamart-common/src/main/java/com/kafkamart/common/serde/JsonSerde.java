package com.kafkamart.common.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

/** Jackson JSON {@link Serde} for KafkaMart event records (ISO-8601 instants). */
public final class JsonSerde {
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private JsonSerde() {}

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static <T> Serde<T> of(Class<T> type) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(type));
    }

    public static final class JsonSerializer<T> implements Serializer<T> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception failure) {
                throw new SerializationException(
                        "JSON serialize failed for topic " + topic, failure);
            }
        }

        @Override
        public void close() {}
    }

    public static final class JsonDeserializer<T> implements Deserializer<T> {
        private final Class<T> type;

        public JsonDeserializer(Class<T> type) {
            this.type = type;
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            try {
                return MAPPER.readValue(data, type);
            } catch (Exception failure) {
                throw new SerializationException(
                        "JSON deserialize failed for topic "
                                + topic
                                + " payload="
                                + new String(data, StandardCharsets.UTF_8),
                        failure);
            }
        }

        @Override
        public void close() {}
    }
}
