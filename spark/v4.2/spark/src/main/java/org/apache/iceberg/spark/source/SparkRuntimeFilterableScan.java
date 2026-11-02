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
package org.apache.iceberg.spark.source;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkV2Filters;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.optimizer.NestedDynamicPruning;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SparkRuntimeFilterableScan extends SparkPartitioningAwareScan<PartitionScanTask>
    implements SupportsRuntimeV2Filtering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeFilterableScan.class);

  // InclusiveMetricsEvaluator reads no value bounds for an IN with more literals than this
  private static final int METRICS_IN_LIMIT = 200;

  private final SparkReadConf readConf;
  private final boolean dataColumnFilters;
  private final List<Expression> runtimeFilters;
  private final List<Predicate> pushedPredicates;
  private Expression readerRuntimeFilter = Expressions.alwaysTrue();

  protected SparkRuntimeFilterableScan(
      SparkSession spark,
      Table table,
      Schema schema,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters,
      List<PartitionPredicate> partitionPredicates,
      Supplier<ScanReport> scanReportSupplier,
      boolean dataColumnFilters) {
    super(
        spark,
        table,
        schema,
        scan,
        readConf,
        projection,
        filters,
        partitionPredicates,
        scanReportSupplier);
    this.readConf = readConf;
    this.dataColumnFilters = dataColumnFilters;
    this.runtimeFilters = Lists.newArrayList();
    this.pushedPredicates = Lists.newArrayList();
  }

  @Override
  protected Class<PartitionScanTask> taskJavaClass() {
    return PartitionScanTask.class;
  }

  @Override
  public NamedReference[] filterAttributes() {
    Set<Integer> partitionFieldSourceIds = Sets.newHashSet();

    for (PartitionSpec spec : specs()) {
      for (PartitionField field : spec.fields()) {
        partitionFieldSourceIds.add(field.sourceId());
      }
    }

    Map<Integer, String> quotedNameById = SparkSchemaUtil.indexQuotedNameById(projection());

    // the optimizer will look for an equality condition with filter attributes in a join
    // as the scan has been already planned, filtering can only be done on projected attributes
    // that's why only fields that are part of the read schema can be reported

    Set<Integer> filterFieldIds = Sets.newLinkedHashSet();
    partitionFieldSourceIds.stream()
        .filter(fieldId -> projection().findField(fieldId) != null)
        .forEach(filterFieldIds::add);

    if (dataColumnFilters) {
      dataFilterColumns(projection()).forEach(field -> filterFieldIds.add(field.fieldId()));
    }

    return filterFieldIds.stream()
        .map(fieldId -> Spark3Util.toNamedReference(quotedNameById.get(fieldId)))
        .toArray(NamedReference[]::new);
  }

  /**
   * Whether scans offer data columns to runtime filtering, not only partition source columns.
   *
   * <p>Data columns can only prune when the nested dynamic pruning rule of the Iceberg session
   * extensions stops pruning subqueries from nesting into each other. Their runtime filters drop
   * files by their column metrics, and reach the file readers with the residual.
   */
  static boolean offersDataColumns(SparkSession spark, SparkReadConf readConf) {
    return readConf.dataColumnRuntimeFilteringEnabled() && NestedDynamicPruning.installed(spark);
  }

  /** The data columns of {@code projection} that scans offer to runtime filtering. */
  static List<Types.NestedField> dataFilterColumns(Schema projection) {
    return projection.columns().stream()
        .filter(field -> field.type().isPrimitiveType())
        .filter(field -> !MetadataColumns.isMetadataColumn(field.fieldId()))
        .collect(Collectors.toList());
  }

  @Override
  protected synchronized Expression readerRuntimeFilter() {
    return readerRuntimeFilter;
  }

  @Override
  public boolean supportsIterativePushdown() {
    return true;
  }

  @Override
  public synchronized Predicate[] pushedPredicates() {
    return pushedPredicates.toArray(new Predicate[0]);
  }

  @Override
  public synchronized void filter(Predicate[] predicates) {
    List<PartitionPredicate> partitionPredicates = Lists.newArrayList();
    List<Predicate> runtimePredicates = Lists.newArrayList();

    for (Predicate predicate : predicates) {
      if (predicate instanceof PartitionPredicate partitionPredicate) {
        partitionPredicates.add(partitionPredicate);
      } else {
        runtimePredicates.add(predicate);
      }
    }

    if (!runtimePredicates.isEmpty()) {
      applyRuntimeFilter(runtimePredicates.toArray(new Predicate[0]));
    }

    if (!partitionPredicates.isEmpty()) {
      applyPartitionPredicates(partitionPredicates);
      pushedPredicates.addAll(partitionPredicates);
    }
  }

  private void applyPartitionPredicates(List<PartitionPredicate> additionalPredicates) {
    partitionPredicates().addAll(additionalPredicates);

    List<PartitionScanTask> filteredTasks = tasksMatchingPartitionPredicates();
    if (filteredTasks.size() < tasks().size()) {
      LOG.info(
          "{} of {} task(s) for table {} matched {} opaque runtime partition predicate(s)",
          filteredTasks.size(),
          tasks().size(),
          table().name(),
          additionalPredicates.size());
      resetTasks(filteredTasks);
    }
  }

  private void applyRuntimeFilter(Predicate[] predicates) {
    Expression runtimeFilter = convertRuntimePredicates(predicates);

    if (runtimeFilter != Expressions.alwaysTrue()) {
      Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();

      for (PartitionSpec spec : specs()) {
        Expression inclusiveExpr =
            Projections.inclusive(spec, caseSensitive()).project(runtimeFilter);
        Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);
        evaluatorsBySpecId.put(spec.specId(), inclusive);
      }

      InclusiveMetricsEvaluator metrics =
          new InclusiveMetricsEvaluator(
              projection(), inAsRange(runtimeFilter, METRICS_IN_LIMIT), caseSensitive());

      List<PartitionScanTask> filteredTasks =
          tasks().stream()
              .filter(
                  task -> {
                    Evaluator evaluator = evaluatorsBySpecId.get(task.spec().specId());
                    return evaluator.eval(task.partition())
                        && (!(task instanceof FileScanTask fileTask)
                            || metrics.eval(fileTask.file()));
                  })
              .collect(Collectors.toList());

      LOG.info(
          "{} of {} task(s) for table {} matched runtime filter `{}`",
          filteredTasks.size(),
          tasks().size(),
          table().name(),
          ExpressionUtil.toSanitizedString(runtimeFilter));

      // don't invalidate tasks if the runtime filter had no effect to avoid planning splits again
      if (filteredTasks.size() < tasks().size()) {
        resetTasks(filteredTasks);
      }

      // save the evaluated filter for equals/hashCode
      runtimeFilters.add(runtimeFilter);

      // readers skip the chunks and row groups the filter rules out, and the Vortex reader drops
      // the rows it rules out, which the join drops anyway
      if (dataColumnFilters) {
        this.readerRuntimeFilter =
            Expressions.and(
                readerRuntimeFilter,
                inAsRange(readerConjuncts(runtimeFilter), readConf.runtimeFilterMaxValues()));
      }
    }
  }

  protected Map<String, DeleteFileSet> rewritableDeletes(boolean forDVs) {
    Map<String, DeleteFileSet> rewritableDeletes = Maps.newHashMap();

    for (ScanTask task : tasks()) {
      FileScanTask fileScanTask = task.asFileScanTask();
      for (DeleteFile deleteFile : fileScanTask.deletes()) {
        if (shouldRewrite(deleteFile, forDVs)) {
          rewritableDeletes
              .computeIfAbsent(fileScanTask.file().location(), ignored -> DeleteFileSet.create())
              .add(deleteFile);
        }
      }
    }

    return rewritableDeletes;
  }

  // for DVs all position deletes must be rewritten
  // for position deletes, only file-scoped deletes must be rewritten
  private boolean shouldRewrite(DeleteFile deleteFile, boolean forDVs) {
    if (forDVs) {
      return deleteFile.content() != FileContent.EQUALITY_DELETES;
    }

    return ContentFileUtil.isFileScoped(deleteFile);
  }

  // at this moment, Spark can only pass IN filters for a single attribute
  // if there are multiple filter attributes, Spark will pass two separate IN filters
  private Expression convertRuntimePredicates(Predicate[] predicates) {
    Expression filter = Expressions.alwaysTrue();

    for (Predicate predicate : predicates) {
      Expression expr = SparkV2Filters.convert(predicate);
      if (expr != null) {
        try {
          Binder.bind(projection().asStruct(), expr, caseSensitive());
          filter = Expressions.and(filter, expr);
          pushedPredicates.add(predicate);
        } catch (ValidationException e) {
          LOG.warn("Failed to bind {} to expected schema, skipping runtime filter", expr, e);
        }
      } else {
        LOG.warn("Unsupported runtime filter {}", predicate);
      }
    }

    return filter;
  }

  /**
   * The conjuncts of {@code filter} that read only top-level data columns of the projection. Every
   * reader reads these columns, so each reader can bind them.
   */
  private Expression readerConjuncts(Expression filter) {
    if (filter instanceof And and) {
      return Expressions.and(readerConjuncts(and.left()), readerConjuncts(and.right()));
    }

    Set<Integer> columnIds =
        dataFilterColumns(projection()).stream()
            .map(Types.NestedField::fieldId)
            .collect(Collectors.toSet());
    Set<Integer> referencedIds =
        Binder.boundReferences(projection().asStruct(), List.of(filter), caseSensitive());
    return columnIds.containsAll(referencedIds) ? filter : Expressions.alwaysTrue();
  }

  /**
   * Replaces each IN conjunct with more than {@code maxValues} literals by the range between its
   * smallest and largest literal. Every value of the IN satisfies the range, so the result keeps
   * every row the filter keeps.
   */
  private static Expression inAsRange(Expression filter, int maxValues) {
    if (filter instanceof And and) {
      return Expressions.and(inAsRange(and.left(), maxValues), inAsRange(and.right(), maxValues));
    }

    if (!(filter instanceof UnboundPredicate<?> pred)
        || pred.op() != Expression.Operation.IN
        || pred.literals().size() <= maxValues) {
      return filter;
    }

    return range(pred);
  }

  private static <T> Expression range(UnboundPredicate<T> pred) {
    Comparator<T> comparator = pred.literals().get(0).comparator();
    T min = null;
    T max = null;
    for (Literal<T> literal : pred.literals()) {
      T value = literal.value();
      min = min == null || comparator.compare(value, min) < 0 ? value : min;
      max = max == null || comparator.compare(value, max) > 0 ? value : max;
    }

    return Expressions.and(
        Expressions.greaterThanOrEqual(pred.term(), min),
        Expressions.lessThanOrEqual(pred.term(), max));
  }

  protected String runtimeFiltersDesc() {
    return Spark3Util.describe(runtimeFilters);
  }
}
