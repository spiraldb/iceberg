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
package org.apache.iceberg.io.datafile;

import java.util.function.Predicate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDeleteIndex;

/**
 * Used for filtering out deleted records on the reader level. Currently only used by the Spark
 * vectorized readers.
 *
 * @param <D> type of the records which are filtered
 */
public interface DeleteFilter<D> {
  /** The schema required to apply the delete filter. */
  Schema requiredSchema();

  /** Is there any positional delete files to apply. */
  boolean hasPosDeletes();

  /** Is there any equality delete files to apply. */
  boolean hasEqDeletes();

  /** The rows deleted by positional deletes. */
  PositionDeleteIndex deletedRowPositions();

  /** The filter to apply for the equality deletes. */
  Predicate<D> eqDeletedRowFilter();

  /** Should be called if a row is removed. */
  void incrementDeleteCount();

  /** The projected schema. */
  Schema expectedSchema();
}
