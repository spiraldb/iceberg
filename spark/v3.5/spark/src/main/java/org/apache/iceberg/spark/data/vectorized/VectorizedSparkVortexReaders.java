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
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.vortex.VortexBatchReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class VectorizedSparkVortexReaders {
  private VectorizedSparkVortexReaders() {}

  // TODO(aduffy): patch in idToConstant all over the place.
  public static VortexBatchReader<ColumnarBatch> buildReader(
          Schema icebergSchema, DType vortexSchema, Map<Integer, ?> idToConstant, DeleteFilter<InternalRow> deleteFilter) {
    // TODO(aduffy): schema compat, idToConstant handling.
    return new SchemaCachingBatchReader(deleteFilter);
  }

  static final class SchemaCachingBatchReader implements VortexBatchReader<ColumnarBatch> {
    // Reusable vector schema root.
    private VectorSchemaRoot root;
    private final DeleteFilter<InternalRow> deleteFilter;
    // TODO(os): we should keep track of start pos from the array, by getting its offset
    private long rowStartPosInBatch = 0;
    private boolean hasIsDeletedColumn = false;
    
    SchemaCachingBatchReader(DeleteFilter<InternalRow> deleteFilter) {
      this.deleteFilter = deleteFilter;
      this.rowStartPosInBatch = 0;
      if (deleteFilter != null) {
        this.hasIsDeletedColumn = deleteFilter.requiredSchema().findField(MetadataColumns.IS_DELETED.fieldId()) != null;

      }
    }

    @Override
    public ColumnarBatch read(Array batch) {
      this.root = batch.exportToArrow(ArrowAllocation.rootAllocator(), this.root);
      int rowCount = this.root.getRowCount();
      ColumnVector[] vectors = new ColumnVector[this.root.getFieldVectors().size()];

      for (int i = 0; i < this.root.getFieldVectors().size(); ++i) {
        vectors[i] = new VortexArrowColumnVector(this.root.getFieldVectors().get(i));
      }
      
      // Create the batch first
      ColumnarBatch columnarBatch = new VortexColumnarBatch(batch, vectors, rowCount);
      
      if (deleteFilter != null) {
        if (hasIsDeletedColumn) {
          boolean[] isDeleted = ColumnarBatchUtil.buildIsDeleted(vectors, deleteFilter, rowStartPosInBatch, rowCount);
          for (ColumnVector vector : vectors) {
            if (vector instanceof DeletedColumnVector) {
              ((DeletedColumnVector) vector).setValue(isDeleted);
            }
          }
        } else {
          Pair<int[], Integer> pair =
              ColumnarBatchUtil.buildRowIdMapping(vectors, deleteFilter, rowStartPosInBatch, rowCount);
          
          if (pair != null) {
            // Some rows were deleted, create a filtered batch
            int[] rowIdMapping = pair.first();
            int numLiveRows = pair.second();
            
            ColumnVector[] filteredVectors = new ColumnVector[vectors.length];
            for (int i = 0; i < vectors.length; i++) {
              filteredVectors[i] = new ColumnVectorWithFilter(vectors[i], rowIdMapping);
            }
            
            // Remove any extra columns that were only needed for equality deletes
            if (deleteFilter.hasEqDeletes()) {
              filteredVectors = ColumnarBatchUtil.removeExtraColumns(deleteFilter, filteredVectors);
            }
            
            columnarBatch = new ColumnarBatch(filteredVectors);
            columnarBatch.setNumRows(numLiveRows);
          }
        }
      }

      rowStartPosInBatch += rowCount;
      return columnarBatch;
    }
  }
}
