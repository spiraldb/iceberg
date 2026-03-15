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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.BaseFormatModel;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.InputFile;

public class VortexFormatModel<D, S>
    extends BaseFormatModel<D, S, Void, VortexBatchReader<?>, DType> {

  private final Vortex.BatchReaderFunction<D> batchReaderFunction;
  private final Vortex.ReaderFunction<D> rowReaderFunction;

  public static <D, S> VortexFormatModel<D, S> forRowReader(
      Class<D> type,
      Class<S> schemaType,
      Vortex.ReaderFunction<D> readerFunction) {
    return new VortexFormatModel<>(type, schemaType, readerFunction, null);
  }

  public static <D, S> VortexFormatModel<D, S> forBatchReader(
      Class<D> type,
      Class<S> schemaType,
      Vortex.BatchReaderFunction<D> batchReaderFunction) {
    return new VortexFormatModel<>(type, schemaType, null, batchReaderFunction);
  }

  private VortexFormatModel(
      Class<D> type,
      Class<S> schemaType,
      Vortex.ReaderFunction<D> rowReaderFunction,
      Vortex.BatchReaderFunction<D> batchReaderFunction) {
    super(type, schemaType, null, null);
    this.rowReaderFunction = rowReaderFunction;
    this.batchReaderFunction = batchReaderFunction;
  }

  @Override
  public FileFormat format() {
    return FileFormat.VORTEX;
  }

  @Override
  public ModelWriteBuilder<D, S> writeBuilder(EncryptedOutputFile outputFile) {
    throw new UnsupportedOperationException("Vortex writing is not yet supported");
  }

  @Override
  public ReadBuilder<D, S> readBuilder(InputFile inputFile) {
    if (batchReaderFunction != null) {
      return Vortex.ReadBuilderWrapper.forBatchReader(inputFile, batchReaderFunction);
    } else {
      return Vortex.ReadBuilderWrapper.forRowReader(inputFile, rowReaderFunction);
    }
  }

  public static <D, S> ReadBuilder<D, S> rowReadBuilder(
      InputFile inputFile, Vortex.ReaderFunction<D> readerFunction) {
    return Vortex.ReadBuilderWrapper.forRowReader(inputFile, readerFunction);
  }

  public static <D, S> ReadBuilder<D, S> batchReadBuilder(
      InputFile inputFile, Vortex.BatchReaderFunction<D> batchReaderFunction) {
    return Vortex.ReadBuilderWrapper.forBatchReader(inputFile, batchReaderFunction);
  }
}
