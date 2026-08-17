package com.kafkamart.connect;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * Turns schemaless JSON ({@code JsonConverter} + {@code schemas.enable=false} → {@link Map}) into
 * a Connect {@link Struct} so JDBC sink can create/upsert columns. Nested {@code items} arrays are
 * dropped by default (relational sinks cannot store them without flattening).
 */
public abstract class InferConnectSchema<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String EXCLUDE_CONFIG = "exclude";

    public static final ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            EXCLUDE_CONFIG,
                            ConfigDef.Type.LIST,
                            List.of("items"),
                            ConfigDef.Importance.MEDIUM,
                            "Value fields to drop before inferring a struct schema");

    private Set<String> exclude = Set.of("items");

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        List<String> names = config.getList(EXCLUDE_CONFIG);
        this.exclude = new LinkedHashSet<>(names);
    }

    @Override
    public R apply(R record) {
        Object raw = operatingValue(record);
        if (raw == null) {
            return record;
        }
        if (raw instanceof Struct) {
            return record;
        }
        if (!(raw instanceof Map<?, ?> map)) {
            return record;
        }
        Schema schema = schemaFor(map);
        Struct struct = structFor(schema, map);
        return newRecord(record, schema, struct);
    }

    private Schema schemaFor(Map<?, ?> map) {
        SchemaBuilder builder = SchemaBuilder.struct().optional();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            String field = String.valueOf(entry.getKey());
            if (exclude.contains(field) || entry.getValue() == null) {
                continue;
            }
            builder.field(field, schemaOf(entry.getValue()).optional().build());
        }
        return builder.build();
    }

    private Struct structFor(Schema schema, Map<?, ?> map) {
        Struct struct = new Struct(schema);
        schema.fields()
                .forEach(
                        field -> {
                            Object value = map.get(field.name());
                            if (value != null) {
                                struct.put(field, coerce(field.schema(), value));
                            }
                        });
        return struct;
    }

    private static SchemaBuilder schemaOf(Object value) {
        if (value instanceof Boolean) {
            return SchemaBuilder.bool();
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return SchemaBuilder.int32();
        }
        if (value instanceof Long) {
            return SchemaBuilder.int64();
        }
        if (value instanceof Float || value instanceof Double || value instanceof BigDecimal) {
            return SchemaBuilder.float64();
        }
        return SchemaBuilder.string();
    }

    private static Object coerce(Schema schema, Object value) {
        return switch (schema.type()) {
            case BOOLEAN -> value instanceof Boolean b ? b : Boolean.parseBoolean(String.valueOf(value));
            case INT32 -> value instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(value));
            case INT64 -> value instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(value));
            case FLOAT64 ->
                    value instanceof Number n
                            ? n.doubleValue()
                            : Double.parseDouble(String.valueOf(value));
            default -> String.valueOf(value);
        };
    }

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema schema, Object value);

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    public static final class Key<R extends ConnectRecord<R>> extends InferConnectSchema<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema schema, Object value) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    schema,
                    value,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends InferConnectSchema<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema schema, Object value) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    schema,
                    value,
                    record.timestamp());
        }
    }
}
