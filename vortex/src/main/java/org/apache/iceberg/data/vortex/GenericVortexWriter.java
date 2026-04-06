/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.data.vortex;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.vortex.VortexValueWriter;

/** Writes Iceberg generic {@link Record} objects to Arrow vectors for Vortex file output. */
public class GenericVortexWriter implements VortexValueWriter<Record> {
  private static final OffsetDateTime EPOCH =
      OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final LocalDateTime LOCAL_EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0);

  private final List<Types.NestedField> columns;

  private GenericVortexWriter(Schema schema) {
    this.columns = schema.columns();
  }

  public static VortexValueWriter<Record> buildWriter(Schema schema) {
    return new GenericVortexWriter(schema);
  }

  @Override
  public void write(Record datum, VectorSchemaRoot root, int rowIndex) {
    for (int fieldIndex = 0; fieldIndex < columns.size(); fieldIndex++) {
      Types.NestedField field = columns.get(fieldIndex);
      FieldVector vector = root.getVector(fieldIndex);
      Object value = datum.get(fieldIndex);

      if (value == null) {
        vector.setNull(rowIndex);
        continue;
      }

      writeValue(vector, field.type(), value, rowIndex);
    }
  }

  @SuppressWarnings("CyclomaticComplexity")
  private static void writeValue(
      FieldVector vector, org.apache.iceberg.types.Type type, Object value, int rowIndex) {
    switch (type.typeId()) {
      case BOOLEAN:
        ((BitVector) vector).setSafe(rowIndex, ((Boolean) value) ? 1 : 0);
        break;
      case INTEGER:
        ((IntVector) vector).setSafe(rowIndex, (Integer) value);
        break;
      case LONG:
        ((BigIntVector) vector).setSafe(rowIndex, (Long) value);
        break;
      case FLOAT:
        ((Float4Vector) vector).setSafe(rowIndex, (Float) value);
        break;
      case DOUBLE:
        ((Float8Vector) vector).setSafe(rowIndex, (Double) value);
        break;
      case STRING:
        byte[] strBytes = value.toString().getBytes(StandardCharsets.UTF_8);
        ((VarCharVector) vector).setSafe(rowIndex, strBytes);
        break;
      case BINARY:
      case FIXED:
        byte[] bytes;
        if (value instanceof ByteBuffer) {
          bytes = ByteBuffers.toByteArray((ByteBuffer) value);
        } else {
          bytes = (byte[]) value;
        }

        ((VarBinaryVector) vector).setSafe(rowIndex, bytes);
        break;
      case DECIMAL:
        ((DecimalVector) vector).setSafe(rowIndex, (BigDecimal) value);
        break;
      case DATE:
        int epochDay = (int) ((LocalDate) value).toEpochDay();
        ((DateDayVector) vector).setSafe(rowIndex, epochDay);
        break;
      case TIME:
        long timeMicros = ((LocalTime) value).getLong(java.time.temporal.ChronoField.MICRO_OF_DAY);
        ((TimeMicroVector) vector).setSafe(rowIndex, timeMicros);
        break;
      case TIMESTAMP:
        Types.TimestampType tsType = (Types.TimestampType) type;
        if (tsType.shouldAdjustToUTC()) {
          long epochMicros = ChronoUnit.MICROS.between(EPOCH, (OffsetDateTime) value);
          ((TimeStampMicroTZVector) vector).setSafe(rowIndex, epochMicros);
        } else {
          long localEpochMicros = ChronoUnit.MICROS.between(LOCAL_EPOCH, (LocalDateTime) value);
          ((TimeStampMicroVector) vector).setSafe(rowIndex, localEpochMicros);
        }

        break;
      case TIMESTAMP_NANO:
        Types.TimestampNanoType tsNanoType = (Types.TimestampNanoType) type;
        if (tsNanoType.shouldAdjustToUTC()) {
          long epochNanos = ChronoUnit.NANOS.between(EPOCH, (OffsetDateTime) value);
          ((TimeStampNanoTZVector) vector).setSafe(rowIndex, epochNanos);
        } else {
          long localEpochNanos = ChronoUnit.NANOS.between(LOCAL_EPOCH, (LocalDateTime) value);
          ((TimeStampNanoVector) vector).setSafe(rowIndex, localEpochNanos);
        }

        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported Iceberg type for Vortex write: " + type);
    }
  }
}
