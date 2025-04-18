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
package org.apache.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * A base writer factory to be extended by query engine integrations.
 *
 * @param <T> type of the engine specific records
 * @param <S> type of the engine specific schema
 */
public abstract class RegistryBasedFileWriterFactory<T, S> implements FileWriterFactory<T> {
  private final Table table;
  private final FileFormat dataFileFormat;
  private final String inputType;
  private final Schema dataSchema;
  private final SortOrder dataSortOrder;
  private final FileFormat deleteFileFormat;
  private final int[] equalityFieldIds;
  private final Schema equalityDeleteRowSchema;
  private final SortOrder equalityDeleteSortOrder;
  private final Schema positionDeleteRowSchema;
  private final Map<String, String> writeProperties;
  private final S rowSchemaType;
  private final S equalityDeleteSchemaType;
  private final S positionalDeleteSchemaType;

  protected RegistryBasedFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      String inputType,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema,
      Map<String, String> writeProperties,
      S rowSchemaType,
      S equalityDeleteSchemaType,
      S positionalDeleteSchemaType) {
    this.table = table;
    this.dataFileFormat = dataFileFormat;
    this.inputType = inputType;
    this.dataSchema = dataSchema;
    this.dataSortOrder = dataSortOrder;
    this.deleteFileFormat = deleteFileFormat;
    this.equalityFieldIds = equalityFieldIds;
    this.equalityDeleteRowSchema = equalityDeleteRowSchema;
    this.equalityDeleteSortOrder = equalityDeleteSortOrder;
    this.positionDeleteRowSchema = positionDeleteRowSchema;
    this.writeProperties = writeProperties != null ? writeProperties : ImmutableMap.of();
    this.rowSchemaType = rowSchemaType;
    this.equalityDeleteSchemaType = equalityDeleteSchemaType;
    this.positionalDeleteSchemaType = positionalDeleteSchemaType;
  }

  protected S rowSchemaType() {
    return rowSchemaType;
  }

  protected S equalityDeleteRowSchemaType() {
    return equalityDeleteSchemaType;
  }

  protected S positionDeleteRowSchemaType() {
    return positionalDeleteSchemaType;
  }

  @Override
  public DataWriter<T> newDataWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Map<String, String> properties = table.properties();
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);

    try {
      return ObjectModelRegistry.writerBuilder(dataFileFormat, inputType, file)
          .schema(dataSchema)
          .set(properties)
          .set(writeProperties)
          .metricsConfig(metricsConfig)
          .engineSchema(rowSchemaType())
          .withSpec(spec)
          .withPartition(partition)
          .withKeyMetadata(keyMetadata)
          .withSortOrder(dataSortOrder)
          .overwrite()
          .dataWriter();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public EqualityDeleteWriter<T> newEqualityDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Map<String, String> properties = table.properties();
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);

    try {
      return ObjectModelRegistry.equalityDeleteWriterBuilder(deleteFileFormat, inputType, file)
          .set(properties)
          .set(writeProperties)
          .metricsConfig(metricsConfig)
          .engineSchema(equalityDeleteRowSchemaType())
          .withRowSchema(equalityDeleteRowSchema)
          .withEqualityFieldIds(equalityFieldIds)
          .withSpec(spec)
          .withPartition(partition)
          .withKeyMetadata(keyMetadata)
          .withSortOrder(equalityDeleteSortOrder)
          .overwrite()
          .equalityDeleteWriter();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new equality delete writer", e);
    }
  }

  @Override
  public PositionDeleteWriter<T> newPositionDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Map<String, String> properties = table.properties();
    MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);

    try {
      return ObjectModelRegistry.positionDeleteWriterBuilder(deleteFileFormat, inputType, file)
          .set(properties)
          .set(writeProperties)
          .metricsConfig(metricsConfig)
          .engineSchema(positionDeleteRowSchemaType())
          .withRowSchema(positionDeleteRowSchema)
          .withSpec(spec)
          .withPartition(partition)
          .withKeyMetadata(keyMetadata)
          .overwrite()
          .positionDeleteWriter();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new position delete writer", e);
    }
  }

  protected Schema dataSchema() {
    return dataSchema;
  }

  protected Schema equalityDeleteRowSchema() {
    return equalityDeleteRowSchema;
  }

  protected Schema positionDeleteRowSchema() {
    return positionDeleteRowSchema;
  }
}
