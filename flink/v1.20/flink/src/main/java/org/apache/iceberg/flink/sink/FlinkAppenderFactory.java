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
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_ROW_FIELD_NAME;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.flink.data.FlinkOrcWriter;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.datafile.DataFileServiceRegistry;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlinkAppenderFactory implements FileAppenderFactory<RowData>, Serializable {
  private final Schema schema;
  private final RowType flinkSchema;
  private final Map<String, String> props;
  private final PartitionSpec spec;
  private final int[] equalityFieldIds;
  private final Schema eqDeleteRowSchema;
  private final Schema posDeleteRowSchema;
  private final Table table;

  private RowType eqDeleteFlinkSchema = null;
  private RowType posDeleteFlinkSchema = null;

  public static void register() {
    DataFileServiceRegistry.registerAppender(
        FileFormat.AVRO,
        RowData.class.getName(),
        outputFile ->
            Avro.write(outputFile)
                .writerFunction((unused, rowType) -> new FlinkAvroWriter((RowType) rowType))
                .deleteRowWriterFunction(
                    (unused, rowType) ->
                        new FlinkAvroWriter(
                            (RowType)
                                ((RowType) rowType)
                                    .getTypeAt(
                                        ((RowType) rowType)
                                            .getFieldIndex(DELETE_FILE_ROW_FIELD_NAME)))));

    DataFileServiceRegistry.registerAppender(
        FileFormat.PARQUET,
        RowData.class.getName(),
        outputFile ->
            Parquet.write(outputFile)
                .writerFunction(
                    (engineType, messageType) ->
                        FlinkParquetWriters.buildWriter((LogicalType) engineType, messageType))
                .pathTransformFunc(path -> StringData.fromString(path.toString())));

    DataFileServiceRegistry.registerAppender(
        FileFormat.ORC,
        RowData.class.getName(),
        outputFile ->
            ORC.write(outputFile)
                .writerFunction(
                    (schema, messageType, nativeSchema) ->
                        FlinkOrcWriter.buildWriter((RowType) nativeSchema, schema))
                .pathTransformFunc(path -> StringData.fromString(path.toString())));
  }

  public FlinkAppenderFactory(
      Table table,
      Schema schema,
      RowType flinkSchema,
      Map<String, String> props,
      PartitionSpec spec,
      int[] equalityFieldIds,
      Schema eqDeleteRowSchema,
      Schema posDeleteRowSchema) {
    Preconditions.checkNotNull(table, "Table shouldn't be null");
    this.table = table;
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.props = props;
    this.spec = spec;
    this.equalityFieldIds = equalityFieldIds;
    this.eqDeleteRowSchema = eqDeleteRowSchema;
    this.posDeleteRowSchema = posDeleteRowSchema;
  }

  private RowType lazyEqDeleteFlinkSchema() {
    if (eqDeleteFlinkSchema == null) {
      Preconditions.checkNotNull(eqDeleteRowSchema, "Equality delete row schema shouldn't be null");
      this.eqDeleteFlinkSchema = FlinkSchemaUtil.convert(eqDeleteRowSchema);
    }
    return eqDeleteFlinkSchema;
  }

  private RowType lazyPosDeleteFlinkSchema() {
    if (posDeleteFlinkSchema == null) {
      this.posDeleteFlinkSchema =
          FlinkSchemaUtil.convert(DeleteSchemaUtil.posDeleteSchema(posDeleteRowSchema));
    }
    return this.posDeleteFlinkSchema;
  }

  @Override
  public FileAppender<RowData> newAppender(OutputFile outputFile, FileFormat format) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      return DataFileServiceRegistry.writeBuilder(
              format, RowData.class.getName(), EncryptedFiles.plainAsEncryptedOutput(outputFile))
          .engineSchema(flinkSchema)
          .set(props)
          .schema(schema)
          .metricsConfig(metricsConfig)
          .overwrite()
          .appender();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public DataWriter<RowData> newDataWriter(
      EncryptedOutputFile file, FileFormat format, StructLike partition) {
    return new DataWriter<>(
        newAppender(file.encryptingOutputFile(), format),
        format,
        file.encryptingOutputFile().location(),
        spec,
        partition,
        file.keyMetadata());
  }

  @Override
  public EqualityDeleteWriter<RowData> newEqDeleteWriter(
      EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    Preconditions.checkState(
        equalityFieldIds != null && equalityFieldIds.length > 0,
        "Equality field ids shouldn't be null or empty when creating equality-delete writer");
    Preconditions.checkNotNull(
        eqDeleteRowSchema,
        "Equality delete row schema shouldn't be null when creating equality-delete writer");

    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      return DataFileServiceRegistry.writeBuilder(format, RowData.class.getName(), outputFile)
          .overwrite()
          .set(props)
          .metricsConfig(metricsConfig)
          .engineSchema(lazyEqDeleteFlinkSchema())
          .withPartition(partition)
          .withRowSchema(eqDeleteRowSchema)
          .withSpec(spec)
          .withKeyMetadata(outputFile.keyMetadata())
          .withEqualityFieldIds(equalityFieldIds)
          .equalityDeleteWriter();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public PositionDeleteWriter<RowData> newPosDeleteWriter(
      EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);
    try {
      return DataFileServiceRegistry.writeBuilder(format, RowData.class.getName(), outputFile)
          .overwrite()
          .set(props)
          .metricsConfig(metricsConfig)
          .engineSchema(lazyPosDeleteFlinkSchema())
          .withPartition(partition)
          .withRowSchema(posDeleteRowSchema)
          .withSpec(spec)
          .withKeyMetadata(outputFile.keyMetadata())
          .positionDeleteWriter();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
