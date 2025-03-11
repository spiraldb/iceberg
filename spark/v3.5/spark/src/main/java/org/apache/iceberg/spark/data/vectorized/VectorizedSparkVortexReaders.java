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
package org.apache.iceberg.spark.data.vectorized;

import dev.vortex.api.Array;
import dev.vortex.api.DType;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.vortex.VortexBatchReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class VectorizedSparkVortexReaders {
  private VectorizedSparkVortexReaders() {}

  // TODO(aduffy): patch in idToConstant all over the place.
  public static VortexBatchReader<ColumnarBatch> buildReader(
      Schema icebergSchema, DType vortexSchema, Map<Integer, ?> idToConstant) {
    return new VortexBatchReader<ColumnarBatch>() {
      @Override
      public ColumnarBatch read(Array batch) {
        int fieldCount = batch.getDataType().getFieldNames().size();
        ColumnVector[] vectors = new ColumnVector[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
          vectors[i] = new VortexColumnVector(batch.getField(i));
        }

        return new ColumnarBatch(vectors);
      }
    };
  }

  // Builders for different column vectors
  static class VortexColumnVector extends ColumnVector {
    private Array array;

    private VortexColumnVector(Array batch) {
      super(DataTypes.BinaryType);
    }

    @Override
    public void close() {
      if (this.array != null) {
        this.array.close();
        this.array = null;
      }
    }

    @Override
    public boolean hasNull() {
      return array.getDataType().isNullable();
    }

    @Override
    public int numNulls() {
      return array.getNullCount();
    }

    @Override
    public boolean isNullAt(int i) {
      return array.getNull(i);
    }

    @Override
    public boolean getBoolean(int i) {
      return array.getBool(i);
    }

    @Override
    public byte getByte(int i) {
      return array.getByte(i);
    }

    @Override
    public short getShort(int i) {
      return array.getShort(i);
    }

    @Override
    public int getInt(int i) {
      return array.getInt(i);
    }

    @Override
    public long getLong(int i) {
      return array.getLong(i);
    }

    @Override
    public float getFloat(int i) {
      return array.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
      return array.getDouble(i);
    }

    @Override
    public ColumnarArray getArray(int i) {
      throw new UnsupportedOperationException("getArray not supported yet for Vortex");
    }

    @Override
    public ColumnarMap getMap(int i) {
      throw new UnsupportedOperationException("getMap not supported yet for Vortex");
    }

    @Override
    public Decimal getDecimal(int i, int i1, int i2) {
      throw new UnsupportedOperationException("getDecimal not supported for Vortex");
    }

    @Override
    public UTF8String getUTF8String(int i) {
      return UTF8String.fromString(array.getUTF8(i));
    }

    @Override
    public byte[] getBinary(int i) {
      return array.getBinary(i);
    }

    @Override
    public ColumnVector getChild(int i) {
      return new VortexColumnVector(array.getField(i));
    }
  }

  static DataType toSparkType(DType vortexType) {
    switch (vortexType.getVariant()) {
      case NULL:
        throw new UnsupportedOperationException("Vortex NULL type has not equivalent in Spark");
      case BOOL:
        return DataTypes.BooleanType;
      case PRIMITIVE_U8:
      case PRIMITIVE_I8:
        return DataTypes.ByteType;
      case PRIMITIVE_U16:
      case PRIMITIVE_I16:
        return DataTypes.ShortType;
      case PRIMITIVE_U32:
      case PRIMITIVE_I32:
        return DataTypes.IntegerType;
      case PRIMITIVE_U64:
      case PRIMITIVE_I64:
        return DataTypes.LongType;
      case PRIMITIVE_F16:
        throw new UnsupportedOperationException("Vortex F16 type has not equivalent in Spark");
      case PRIMITIVE_F32:
        return DataTypes.FloatType;
      case PRIMITIVE_F64:
        return DataTypes.DoubleType;
      case UTF8:
        return DataTypes.StringType;
      case BINARY:
        return DataTypes.BinaryType;
      case STRUCT:
        List<String> fieldNames = vortexType.getFieldNames();
        List<DType> fieldTypes = vortexType.getFieldTypes();

        try {
          StructType structType = new StructType();
          for (int i = 0; i < fieldTypes.size(); i++) {
            String name = fieldNames.get(i);
            DType fieldType = fieldTypes.get(i);
            structType.add(name, toSparkType(fieldType));
          }
          return structType;
        } finally {
          // Cleanup the field DTypes we allocated in call to getFieldTypes above.
          while (!fieldTypes.isEmpty()) {
            fieldTypes.remove(0).close();
          }
        }
      case LIST:
        throw new UnsupportedOperationException("TODO(aduffy): implement LIST type support");
      case EXTENSION:
        // Handle conversion of the Time types.
        if (vortexType.isDate()) {
          return DataTypes.DateType;
        } else if (vortexType.isTimestamp()) {
          if (vortexType.getTimeZone().isEmpty()) {
            return DataTypes.TimestampNTZType;
          } else {
            return DataTypes.TimestampType;
          }
        } else {
          throw new UnsupportedOperationException(
              "Vortex EXTENSION type cannot be converted to Spark");
        }
      default:
        throw new UnsupportedOperationException("Vortex unknown type: " + vortexType);
    }
  }
}
