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
import dev.vortex.arrow.ArrowAllocation;
import dev.vortex.relocated.org.apache.arrow.vector.VectorSchemaRoot;
import dev.vortex.spark.read.VortexArrowColumnVector;
import dev.vortex.spark.read.VortexColumnarBatch;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexBatchReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class VectorizedSparkVortexReaders {
  private VectorizedSparkVortexReaders() {}

  // TODO(aduffy): patch in idToConstant all over the place.
  public static VortexBatchReader<ColumnarBatch> buildReader(
          Schema icebergSchema, DType vortexSchema, Map<Integer, ?> idToConstant, Optional<DeleteFilter<?>> deleteFilter) {
    // TODO(aduffy): schema compat, idToConstant handling.
    // TODO(aduffy): deleteFilter
    // TODO(os): we know we only get DeleteFilter<InternalRow>, cast here or more generics
    return new SchemaCachingBatchReader(icebergSchema, idToConstant);
  }

  static final class SchemaCachingBatchReader implements VortexBatchReader<ColumnarBatch> {
      private final Schema icebergSchema;
      private final Map<Integer, ?> idToConstant;
      // Reusable vector schema root.
    private VectorSchemaRoot root;

    SchemaCachingBatchReader(Schema icebergSchema, Map<Integer, ?> idToConstant) {
        this.icebergSchema = icebergSchema;
        this.idToConstant = idToConstant;
    }

    @Override
    public ColumnarBatch read(Array batch) {
      this.root = batch.exportToArrow(ArrowAllocation.rootAllocator(), this.root);
      int rowCount = this.root.getRowCount();
      ColumnVector[] vectors = new ColumnVector[this.icebergSchema.columns().size()];

      // fill vortex columns
      for (int i = 0; i < this.root.getFieldVectors().size(); ++i) {
        vectors[i] = new VortexArrowColumnVector(this.root.getFieldVectors().get(i));
      }

      // fill constant metadata columns
      for (int i = this.root.getFieldVectors().size(); i < this.icebergSchema.columns().size(); ++i) {
        // assuming all metadata columns come after data columns
        Types.NestedField field = icebergSchema.columns().get(i);
        Object constantValue = idToConstant.get(field.fieldId());
        vectors[i] = new ConstantColumnVector(field.type(), rowCount, constantValue);

      }

      return new VortexColumnarBatch(batch, vectors, rowCount);
    }
  }
}
