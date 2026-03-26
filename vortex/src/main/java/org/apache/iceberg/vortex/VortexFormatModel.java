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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.BaseFormatModel;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;

public class VortexFormatModel<D, S, R> extends BaseFormatModel<D, S, Void, R, DType> {
  private final boolean isBatchReader;

  public interface ReaderFunction<R> {
    VortexRowReader<R> read(Schema schema, DType fileSchema, Map<Integer, ?> idToConstant);
  }

  public interface BatchReaderFunction<T> {
    VortexBatchReader<T> batchRead(
        Schema icebergSchema, DType vortexSchema, Map<Integer, ?> idToConstant);
  }

  public static <D, S> VortexFormatModel<D, S, VortexRowReader<?>> forRowReader(
      Class<D> type, Class<S> schemaType, ReaderFunction<D> readerFunction) {
    return new VortexFormatModel<>(
        type,
        schemaType,
        (icebergSchema, fileSchema, engineSchema, idToConstant) ->
            readerFunction.read(icebergSchema, fileSchema, idToConstant),
        false);
  }

  public static <D, S> VortexFormatModel<D, S, VortexBatchReader<?>> forBatchReader(
      Class<D> type, Class<S> schemaType, BatchReaderFunction<D> batchReaderFunction) {
    return new VortexFormatModel<>(
        type,
        schemaType,
        (icebergSchema, fileSchema, engineSchema, idToConstant) ->
            batchReaderFunction.batchRead(icebergSchema, fileSchema, idToConstant),
        true);
  }

  private VortexFormatModel(
      Class<? extends D> type,
      Class<S> schemaType,
      BaseFormatModel.ReaderFunction<R, S, DType> readerFunction,
      boolean isBatchReader) {
    super(type, schemaType, null, readerFunction);
    this.isBatchReader = isBatchReader;
  }

  @Override
  public FileFormat format() {
    return FileFormat.VORTEX;
  }

  @Override
  public ModelWriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
    throw new UnsupportedOperationException("Vortex write is not supported");
  }

  @Override
  public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    return new ReadBuilderWrapper<>(inputFile, readerFunction(), isBatchReader);
  }

  private static class ReadBuilderWrapper<R, D, S> implements ReadBuilder<D, S> {
    private final InputFile inputFile;
    private final BaseFormatModel.ReaderFunction<R, S, DType> readerFunction;
    private final boolean isBatchReader;
    private Schema schema;
    private S engineSchema;
    private Map<Integer, ?> idToConstant;
    private Optional<Expression> filterPredicate = Optional.empty();
    private long[] rowRange;

    private ReadBuilderWrapper(
        InputFile inputFile,
        BaseFormatModel.ReaderFunction<R, S, DType> readerFunction,
        boolean isBatchReader) {
      this.inputFile = inputFile;
      this.readerFunction = readerFunction;
      this.isBatchReader = isBatchReader;
    }

    @Override
    public ReadBuilder<D, S> split(long newStart, long newLength) {
      this.rowRange = new long[] {newStart, newStart + newLength};
      return this;
    }

    @Override
    public ReadBuilder<D, S> project(Schema projectedSchema) {
      this.schema = projectedSchema;
      return this;
    }

    @Override
    public ReadBuilder<D, S> engineProjection(S newSchema) {
      this.engineSchema = newSchema;
      return this;
    }

    @Override
    public ReadBuilder<D, S> caseSensitive(boolean caseSensitive) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> filter(Expression filter) {
      this.filterPredicate = Optional.of(filter);
      return this;
    }

    @Override
    public ReadBuilder<D, S> set(String key, String value) {
      return this;
    }

    @Override
    public ReadBuilder<D, S> reuseContainers() {
      return this;
    }

    @Override
    public ReadBuilder<D, S> recordsPerBatch(int numRowsPerBatch) {
      if (!isBatchReader) {
        throw new UnsupportedOperationException(
            "Batch reading is not supported in non-vectorized reader");
      }

      return this;
    }

    @Override
    public ReadBuilder<D, S> idToConstant(Map<Integer, ?> newIdToConstant) {
      this.idToConstant = newIdToConstant;
      return this;
    }

    @Override
    public ReadBuilder<D, S> withNameMapping(NameMapping nameMapping) {
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CloseableIterable<D> build() {
      Function<DType, VortexRowReader<D>> readerFunc = null;
      Function<DType, VortexBatchReader<D>> batchReaderFunc = null;

      if (isBatchReader) {
        batchReaderFunc =
            fileSchema ->
                (VortexBatchReader<D>)
                    readerFunction.read(schema, fileSchema, engineSchema, idToConstant);
      } else {
        readerFunc =
            fileSchema ->
                (VortexRowReader<D>)
                    readerFunction.read(schema, fileSchema, engineSchema, idToConstant);
      }

      Schema readSchema = schema;
      if (idToConstant != null) {
        List<Types.NestedField> readerFields =
            schema.columns().stream()
                .filter(field -> !idToConstant.containsKey(field.fieldId()))
                .collect(Collectors.toList());
        readSchema = new Schema(readerFields);
      }

      return new VortexIterable<>(
          inputFile, readSchema, filterPredicate, rowRange, readerFunc, batchReaderFunc);
    }
  }
}
