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
package org.apache.iceberg.flink.source;

import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.ObjectModelRegistry;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkSourceFilter;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.FlinkObjectModels;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

@Internal
public class RowDataFileScanTaskReader implements FileScanTaskReader<RowData> {

  private final Schema tableSchema;
  private final Schema projectedSchema;
  private final String nameMapping;
  private final boolean filterCaseSensitive;
  private final FlinkSourceFilter rowFilter;

  public RowDataFileScanTaskReader(
      Schema tableSchema,
      Schema projectedSchema,
      String nameMapping,
      boolean filterCaseSensitive,
      List<Expression> filters) {
    this.tableSchema = tableSchema;
    this.projectedSchema = projectedSchema;
    this.nameMapping = nameMapping;
    this.filterCaseSensitive = filterCaseSensitive;

    if (filters != null && !filters.isEmpty()) {
      Expression combinedExpression =
          filters.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
      this.rowFilter =
          new FlinkSourceFilter(projectedSchema, combinedExpression, filterCaseSensitive);
    } else {
      this.rowFilter = null;
    }
  }

  @Override
  public CloseableIterator<RowData> open(
      FileScanTask task, InputFilesDecryptor inputFilesDecryptor) {
    Schema partitionSchema = TypeUtil.select(projectedSchema, task.spec().identitySourceIds());

    Map<Integer, ?> idToConstant =
        partitionSchema.columns().isEmpty()
            ? ImmutableMap.of()
            : PartitionUtil.constantsMap(task, RowDataUtil::convertConstant);

    FlinkDeleteFilter deletes =
        new FlinkDeleteFilter(task, tableSchema, projectedSchema, inputFilesDecryptor);
    CloseableIterable<RowData> iterable =
        deletes.filter(
            newIterable(task, deletes.requiredSchema(), idToConstant, inputFilesDecryptor));

    // Project the RowData to remove the extra meta columns.
    if (!projectedSchema.sameSchema(deletes.requiredSchema())) {
      RowDataProjection rowDataProjection =
          RowDataProjection.create(
              deletes.requiredRowType(),
              deletes.requiredSchema().asStruct(),
              projectedSchema.asStruct());
      iterable = CloseableIterable.transform(iterable, rowDataProjection::wrap);
    }

    return iterable.iterator();
  }

  private CloseableIterable<RowData> newIterable(
      FileScanTask task,
      Schema schema,
      Map<Integer, ?> idToConstant,
      InputFilesDecryptor inputFilesDecryptor) {
    CloseableIterable<RowData> iter;
    if (task.isDataTask()) {
      throw new UnsupportedOperationException("Cannot read data task.");
    } else {
      ReadBuilder<?> builder =
          ObjectModelRegistry.readBuilder(
                  task.file().format(),
                  FlinkObjectModels.FLINK_OBJECT_MODEL,
                  inputFilesDecryptor.getInputFile(task))
              .project(schema)
              .constantFieldAccessors(idToConstant)
              .split(task.start(), task.length())
              .filter(task.residual(), filterCaseSensitive)
              .reuseContainers();

      if (nameMapping != null) {
        builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }

      iter = builder.build();
    }

    if (rowFilter != null) {
      return CloseableIterable.filter(iter, rowFilter::filter);
    }
    return iter;
  }

  private static class FlinkDeleteFilter extends DeleteFilter<RowData> {
    private final RowType requiredRowType;
    private final RowDataWrapper asStructLike;
    private final InputFilesDecryptor inputFilesDecryptor;

    FlinkDeleteFilter(
        FileScanTask task,
        Schema tableSchema,
        Schema requestedSchema,
        InputFilesDecryptor inputFilesDecryptor) {
      super(task.file().location(), task.deletes(), tableSchema, requestedSchema);
      this.requiredRowType = FlinkSchemaUtil.convert(requiredSchema());
      this.asStructLike = new RowDataWrapper(requiredRowType, requiredSchema().asStruct());
      this.inputFilesDecryptor = inputFilesDecryptor;
    }

    public RowType requiredRowType() {
      return requiredRowType;
    }

    @Override
    protected StructLike asStructLike(RowData row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return inputFilesDecryptor.getInputFile(location);
    }
  }
}
