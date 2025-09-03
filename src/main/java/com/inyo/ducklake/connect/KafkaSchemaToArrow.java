package com.inyo.ducklake.connect;

import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Field;

import java.util.List;
import java.util.stream.Collectors;

public class KafkaSchemaToArrow {

    public static org.apache.arrow.vector.types.pojo.Schema arrowSchemaFromKafka(Schema kafkaSchema) {
        var fields = kafkaSchema.fields().stream()
                .map(KafkaSchemaToArrow::convertField)
                .collect(Collectors.toList());

        return new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    private static org.apache.arrow.vector.types.pojo.Field convertField(Field kafkaField) {
        String name = kafkaField.name();
        Schema.Type type = kafkaField.schema().type();

        FieldType fieldType = new FieldType(
                kafkaField.schema().isOptional(),
                toArrowType(type),
                null
        );

        if (type == Schema.Type.STRUCT) {
            var children = kafkaField.schema().fields().stream()
                    .map(KafkaSchemaToArrow::convertField)
                    .collect(Collectors.toList());
            return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, children);
        }

        if (type == Schema.Type.ARRAY) {
            var elementField = convertField(
                    new Field("element",0, kafkaField.schema().valueSchema())
            );
            return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, List.of(elementField));
        }

        if (type == Schema.Type.MAP) {
            var keyField = convertField(new Field("key", 0, kafkaField.schema().keySchema()));
            var valueField = convertField(new Field("value", 0,kafkaField.schema().valueSchema()));
            return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, List.of(keyField, valueField));
        }

        return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, null);
    }

    private static ArrowType toArrowType(Schema.Type type) {
        switch (type) {
            case INT8: return new ArrowType.Int(8, true);
            case INT16: return new ArrowType.Int(16, true);
            case INT32: return new ArrowType.Int(32, true);
            case INT64: return new ArrowType.Int(64, true);
            case FLOAT32: return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case FLOAT64: return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case BOOLEAN: return ArrowType.Bool.INSTANCE;
            case STRING: return ArrowType.Utf8.INSTANCE;
            case BYTES: return ArrowType.Binary.INSTANCE;
            case ARRAY: return ArrowType.List.INSTANCE;
            case MAP: return new ArrowType.Map(true);
            case STRUCT: return ArrowType.Struct.INSTANCE;
            default: throw new UnsupportedOperationException("Tipo não suportado: " + type);
        }
    }
}
