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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.datafile.DataFileServiceRegistry;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Factory to create a new {@link FileAppender} to write {@link Record}s. */
public class GenericAppenderFactory implements FileAppenderFactory<Record> {

  private final Schema schema;
  private final PartitionSpec spec;
  private final int[] equalityFieldIds;
  private final Schema eqDeleteRowSchema;
  private final Schema posDeleteRowSchema;
  private final Map<String, String> config = Maps.newHashMap();

  public GenericAppenderFactory(Schema schema) {
    this(schema, PartitionSpec.unpartitioned(), null, null, null);
  }

  public GenericAppenderFactory(Schema schema, PartitionSpec spec) {
    this(schema, spec, null, null, null);
  }

  public GenericAppenderFactory(
      Schema schema,
      PartitionSpec spec,
      int[] equalityFieldIds,
      Schema eqDeleteRowSchema,
      Schema posDeleteRowSchema) {
    this.schema = schema;
    this.spec = spec;
    this.equalityFieldIds = equalityFieldIds;
    this.eqDeleteRowSchema = eqDeleteRowSchema;
    this.posDeleteRowSchema = posDeleteRowSchema;
  }

  public GenericAppenderFactory set(String property, String value) {
    config.put(property, value);
    return this;
  }

  public GenericAppenderFactory setAll(Map<String, String> properties) {
    config.putAll(properties);
    return this;
  }

  @Override
  public FileAppender<Record> newAppender(OutputFile outputFile, FileFormat fileFormat) {
    return newAppender(EncryptionUtil.plainAsEncryptedOutput(outputFile), fileFormat);
  }

  @Override
  public FileAppender<Record> newAppender(
      EncryptedOutputFile encryptedOutputFile, FileFormat fileFormat) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);
    try {
      return DataFileServiceRegistry.writeBuilder(
              fileFormat, Record.class.getName(), encryptedOutputFile)
          .schema(schema)
          .set(config)
          .metricsConfig(metricsConfig)
          .overwrite()
          .appender();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public org.apache.iceberg.io.DataWriter<Record> newDataWriter(
      EncryptedOutputFile file, FileFormat format, StructLike partition) {
    return new org.apache.iceberg.io.DataWriter<>(
        newAppender(file, format),
        format,
        file.encryptingOutputFile().location(),
        spec,
        partition,
        file.keyMetadata());
  }

  @Override
  public EqualityDeleteWriter<Record> newEqDeleteWriter(
      EncryptedOutputFile file, FileFormat format, StructLike partition) {
    Preconditions.checkState(
        equalityFieldIds != null && equalityFieldIds.length > 0,
        "Equality field ids shouldn't be null or empty when creating equality-delete writer");
    Preconditions.checkNotNull(
        eqDeleteRowSchema,
        "Equality delete row schema shouldn't be null when creating equality-delete writer");

    MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);

    try {
      return DataFileServiceRegistry.writeBuilder(format, Record.class.getName(), file)
          .schema(schema)
          .withPartition(partition)
          .overwrite()
          .set(config)
          .metricsConfig(metricsConfig)
          .withRowSchema(eqDeleteRowSchema)
          .withSpec(spec)
          .withKeyMetadata(file.keyMetadata())
          .withEqualityFieldIds(equalityFieldIds)
          .equalityDeleteWriter();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public PositionDeleteWriter<Record> newPosDeleteWriter(
      EncryptedOutputFile file, FileFormat format, StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);

    try {
      return DataFileServiceRegistry.writeBuilder(format, Record.class.getName(), file)
          .schema(schema)
          .withPartition(partition)
          .overwrite()
          .set(config)
          .metricsConfig(metricsConfig)
          .withRowSchema(posDeleteRowSchema)
          .withSpec(spec)
          .withKeyMetadata(file.keyMetadata())
          .positionDeleteWriter();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
