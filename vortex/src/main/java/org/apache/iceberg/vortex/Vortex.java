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
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/** Entrypoint to working with Vortex {@link FileFormat formatted} content files. */
public final class Vortex {
  private Vortex() {}

  static ReadBuilder read(InputFile inputFile) {
    return new ReadBuilder(inputFile);
  }

  public interface ReaderFunction<R> {
    VortexRowReader<R> read(Schema schema, DType fileSchema, Map<Integer, ?> idToConstant);
  }

  public interface BatchReaderFunction<T> {
    VortexBatchReader<T> batchRead(
        Schema icebergSchema, DType vortexSchema, Map<Integer, ?> idToConstant);
  }

  static final class ReadBuilder implements InternalData.ReadBuilder {
    private final InputFile inputFile;
    private Schema schema;
    private ReaderFunction<?> readerFunction;
    private BatchReaderFunction<?> batchReaderFunction;
    private Map<Integer, ?> idToConstant;
    private Optional<Expression> filterPredicate = Optional.empty();

    ReadBuilder(InputFile inputFile) {
      this.inputFile = inputFile;
    }

    @Override
    public ReadBuilder project(Schema projectedSchema) {
      this.schema = projectedSchema;
      return this;
    }

    public ReadBuilder recordsPerBatch(int numRowsPerBatch) {
      // TODO(aduffy): will we care about this?
      return this;
    }

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

    public ReadBuilder filter(Expression newFilter) {
      this.filterPredicate = Optional.of(newFilter);
      return this;
    }

    @Override
    public ReadBuilder reuseContainers() {
      // No-op.
      return this;
    }

    public ReadBuilder idToConstant(Map<Integer, ?> newIdConstant) {
      this.idToConstant = newIdConstant;
      return this;
    }

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

  /**
   * Wrapper that adapts a {@link Vortex.ReadBuilder} (an {@link InternalData.ReadBuilder}
   * implementation) to the {@link org.apache.iceberg.formats.ReadBuilder} interface used by the
   * {@link org.apache.iceberg.formats.FormatModel} API.
   */
  static class ReadBuilderWrapper<D, S>
      implements org.apache.iceberg.formats.ReadBuilder<D, S> {
    private final ReadBuilder internal;
    private final ReaderFunction<?> readerFunction;
    private final BatchReaderFunction<?> batchReaderFunction;
    private Map<Integer, ?> idToConstant = ImmutableMap.of();

    static <D, S> ReadBuilderWrapper<D, S> forRowReader(
        InputFile inputFile, ReaderFunction<D> readerFunction) {
      return new ReadBuilderWrapper<>(inputFile, readerFunction, null);
    }

    static <D, S> ReadBuilderWrapper<D, S> forBatchReader(
        InputFile inputFile, BatchReaderFunction<D> batchReaderFunction) {
      return new ReadBuilderWrapper<>(inputFile, null, batchReaderFunction);
    }

    private ReadBuilderWrapper(
        InputFile inputFile,
        ReaderFunction<?> readerFunction,
        BatchReaderFunction<?> batchReaderFunction) {
      this.internal = Vortex.read(inputFile);
      this.readerFunction = readerFunction;
      this.batchReaderFunction = batchReaderFunction;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> split(long start, long length) {
      internal.split(start, length);
      return this;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> project(Schema schema) {
      internal.project(schema);
      return this;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> engineProjection(S schema) {
      // Vortex does not currently use engine-specific schemas
      return this;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> caseSensitive(boolean caseSensitive) {
      // Vortex does not currently support case-sensitive filtering
      return this;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> filter(Expression filter) {
      internal.filter(filter);
      return this;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> set(String key, String value) {
      internal.set(key, value);
      return this;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> reuseContainers() {
      internal.reuseContainers();
      return this;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> recordsPerBatch(int rowsPerBatch) {
      internal.recordsPerBatch(rowsPerBatch);
      return this;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> idToConstant(
        Map<Integer, ?> newIdToConstant) {
      this.idToConstant = newIdToConstant;
      return this;
    }

    @Override
    public org.apache.iceberg.formats.ReadBuilder<D, S> withNameMapping(NameMapping nameMapping) {
      internal.withNameMapping(nameMapping);
      return this;
    }

    @Override
    public CloseableIterable<D> build() {
      internal.idToConstant(idToConstant);
      if (batchReaderFunction != null) {
        return internal.batchReaderFunction(batchReaderFunction).build();
      } else {
        return internal.readerFunction(readerFunction).build();
      }
    }
  }
}
