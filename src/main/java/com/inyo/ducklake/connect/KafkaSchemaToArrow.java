/*
 * Copyright 2025 Inyo Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inyo.ducklake.connect;

import com.inyo.ducklake.ingestor.ArrowTypeConstants;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class KafkaSchemaToArrow {

  public static org.apache.arrow.vector.types.pojo.Schema arrowSchemaFromKafka(Schema kafkaSchema) {
    var fields =
        kafkaSchema.fields().stream()
            .map(KafkaSchemaToArrow::convertField)
            .collect(Collectors.toList());

    return new org.apache.arrow.vector.types.pojo.Schema(fields);
  }

  private static org.apache.arrow.vector.types.pojo.Field convertField(Field kafkaField) {
    String name = kafkaField.name();
    Schema.Type type = kafkaField.schema().type();

    FieldType fieldType = new FieldType(kafkaField.schema().isOptional(), toArrowType(type), null);

    if (type == Schema.Type.STRUCT) {
      var children =
          kafkaField.schema().fields().stream()
              .map(KafkaSchemaToArrow::convertField)
              .collect(Collectors.toList());
      return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, children);
    }

    if (type == Schema.Type.ARRAY) {
      var elementField = convertField(new Field("element", 0, kafkaField.schema().valueSchema()));
      return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, List.of(elementField));
    }

    if (type == Schema.Type.MAP) {
      var keyFieldSchema = kafkaField.schema().keySchema();
      var valueFieldSchema = kafkaField.schema().valueSchema();
      var arrowKeyField =
          new org.apache.arrow.vector.types.pojo.Field(
              "key", new FieldType(false, toArrowType(keyFieldSchema.type()), null), null);
      // Use recursive conversion for value to capture nested children (STRUCT / ARRAY / MAP)
      org.apache.arrow.vector.types.pojo.Field arrowValueField;
      if (valueFieldSchema.type() == Schema.Type.STRUCT
          || valueFieldSchema.type() == Schema.Type.ARRAY
          || valueFieldSchema.type() == Schema.Type.MAP) {
        arrowValueField = convertField(new Field("value", 0, valueFieldSchema));
      } else {
        arrowValueField =
            new org.apache.arrow.vector.types.pojo.Field(
                "value",
                new FieldType(
                    valueFieldSchema.isOptional(), toArrowType(valueFieldSchema.type()), null),
                null);
      }
      var entriesStruct =
          new org.apache.arrow.vector.types.pojo.Field(
              "entries",
              new FieldType(false, ArrowType.Struct.INSTANCE, null),
              List.of(arrowKeyField, arrowValueField));
      return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, List.of(entriesStruct));
    }

    return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, null);
  }

  private static ArrowType toArrowType(Schema.Type type) {
    return switch (type) {
      case INT8 -> new ArrowType.Int(ArrowTypeConstants.INT8_BIT_WIDTH, true);
      case INT16 -> new ArrowType.Int(ArrowTypeConstants.INT16_BIT_WIDTH, true);
      case INT32 -> new ArrowType.Int(ArrowTypeConstants.INT32_BIT_WIDTH, true);
      case INT64 -> new ArrowType.Int(ArrowTypeConstants.INT64_BIT_WIDTH, true);
      case FLOAT32 -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case FLOAT64 -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case BOOLEAN -> ArrowType.Bool.INSTANCE;
      case STRING -> ArrowType.Utf8.INSTANCE;
      case BYTES -> ArrowType.Binary.INSTANCE;
      case ARRAY -> ArrowType.List.INSTANCE;
      case MAP -> new ArrowType.Map(true);
      case STRUCT -> ArrowType.Struct.INSTANCE;
    };
  }
}
