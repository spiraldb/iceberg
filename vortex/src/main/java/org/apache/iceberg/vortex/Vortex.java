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
package org.apache.iceberg.vortex;

import dev.vortex.api.DType;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.datafile.DataFileServiceRegistry;
import org.apache.iceberg.io.datafile.ReadBuilder;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Entrypoint to working with Vortex {@link FileFormat formatted} content files. */
public final class Vortex {
  private Vortex() {}

  public static void register() {
    // Register generic Vortex reader.
    DataFileServiceRegistry.registerReader(
        FileFormat.VORTEX,
        Record.class.getName(),
        inputFile -> read(inputFile).readerFunction(GenericVortexReader::buildReader));
  }

  public static ReadBuilder read(InputFile inputFile) {
    return new ReadBuilder(inputFile);
  }

  public interface ReaderFunction<R> {
    VortexRowReader<R> read(Schema schema, DType fileSchema, Map<Integer, ?> idToConstant);
  }

  public interface BatchReaderFunction<T> {
    VortexBatchReader<T> batchRead(
        Schema icebergSchema, DType vortexSchema, Map<Integer, ?> idToConstant);
  }

  public static final class ReadBuilder
      implements InternalData.ReadBuilder,
          org.apache.iceberg.io.datafile.ReadBuilder<ReadBuilder, Object> {
    private final InputFile inputFile;
    private Schema schema;
    private ReaderFunction<?> readerFunction;
    private BatchReaderFunction<?> batchReaderFunction;
    private Map<Integer, ?> idToConstant;
    private Optional<Expression> filterPredicate;

    ReadBuilder(InputFile inputFile) {
      this.inputFile = inputFile;
    }

    @Override
    public ReadBuilder project(Schema projectedSchema) {
      this.schema = projectedSchema;
      return this;
    }

    @Override
    public ReadBuilder recordsPerBatch(int numRowsPerBatch) {
      // TODO(aduffy): will we care about this?
      return this;
    }

    @Override
    public ReadBuilder set(String key, String value) {
      // TODO(aduffy): support configuring object store credentials here.
      return this;
    }

    @Override
    public ReadBuilder split(long newStart, long newLength) {
      // TODO(aduffy): support splitting? These are in terms of file bytes, which is pretty
      //  annoying.
      return this;
    }

    @Override
    public ReadBuilder filter(Expression newFilter) {
      // At least print the filter.
      System.err.println("VORTEX: PUSHED FILTER: " + newFilter);
      this.filterPredicate = Optional.of(newFilter);
      return this;
    }

    @Override
    public ReadBuilder reuseContainers() {
      // No-op.
      return this;
    }

    @Override
    public ReadBuilder reuseContainers(boolean newReuseContainers) {
      // This is a no-op.
      return this;
    }

    @Override
    public ReadBuilder idToConstant(Map<Integer, ?> newIdConstant) {
      this.idToConstant = newIdConstant;
      return this;
    }

    @Override
    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      // TODO(aduffy): is this for field renames? Figure out how to patch this through.
      return this;
    }

    @Override
    public InternalData.ReadBuilder setRootType(Class<? extends StructLike> rootClass) {
      throw new UnsupportedOperationException("Custom types are not supported for Vortex.");
    }

    @Override
    public InternalData.ReadBuilder setCustomType(
        int fieldId, Class<? extends StructLike> structClass) {
      throw new UnsupportedOperationException("Custom types are not supported for Vortex");
    }

    public <D> ReadBuilder readerFunction(ReaderFunction<D> newReaderFunc) {
      Preconditions.checkState(
          readerFunction == null, "Cannot set multiple read builder functions");
      this.readerFunction = newReaderFunc;
      return this;
    }

    public <D> ReadBuilder batchReaderFunction(BatchReaderFunction<D> newReaderFunc) {
      Preconditions.checkState(
          readerFunction == null && batchReaderFunction == null,
          "Cannot set multiple read builder functions");
      this.batchReaderFunction = newReaderFunc;
      return this;
    }

    @Override
    public <D> CloseableIterable<D> build() {
      Preconditions.checkState(schema != null, "schema must be configured");
      Preconditions.checkState(
          readerFunction != null || batchReaderFunction != null,
          "must set one of readerFunction, batchReaderFunction");

      Function<DType, VortexRowReader<D>> readerFunc =
          readerFunction == null
              ? null
              : fileSchema ->
                  (VortexRowReader<D>) readerFunction.read(schema, fileSchema, idToConstant);
      Function<DType, VortexBatchReader<D>> batchReaderFunc =
          batchReaderFunction == null
              ? null
              : fileSchema ->
                  (VortexBatchReader<D>)
                      batchReaderFunction.batchRead(schema, fileSchema, idToConstant);

      return new VortexIterable<>(inputFile, schema, filterPredicate, readerFunc, batchReaderFunc);
    }
  }
}
