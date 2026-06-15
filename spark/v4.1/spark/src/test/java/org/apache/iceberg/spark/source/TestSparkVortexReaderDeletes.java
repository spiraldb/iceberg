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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PlanningMode;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.TestTemplate;

/**
 * Exercises the Spark columnar read path for Vortex tables with deletes.
 *
 * <p>Vortex applies all deletes (and residual filters) natively in the scan (see {@code
 * BaseBatchReader.newPushdownBatchIterable}): position deletes are pushed directly, and equality
 * deletes are resolved to positions by a pre-scan and pushed as well. So the position- and
 * equality-delete cases inherited from {@link TestSparkReaderDeletes} run unchanged. Only the
 * {@code _deleted}-metadata-column-with-delete-files cases are skipped: that requires retaining and
 * marking deleted rows, which the drop-only pushdown path does not do.
 */
public class TestSparkVortexReaderDeletes extends TestSparkReaderDeletes {

  @Parameters(name = "fileFormat = {0}, formatVersion = {1}, vectorized = {2}, planningMode = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.VORTEX, 2, true, PlanningMode.DISTRIBUTED},
      new Object[] {FileFormat.VORTEX, 3, true, PlanningMode.LOCAL},
    };
  }

  // Deletes are dropped inside the Vortex scan, so they never reach Spark and are not reflected in
  // the NumDeletes metric. Disable delete-count assertions for this path.
  @Override
  protected boolean countDeletes() {
    return false;
  }

  private static void skipDeletedColumn() {
    Assumptions.abort(
        "Vortex applies deletes via native scan pushdown (drop-only); the _deleted metadata column "
            + "with delete files requires retaining and marking rows, which is not supported");
  }

  @TestTemplate
  @Override
  public void testReadEqualityDeleteRows() {
    // Uses EqualityDeleteRowReader with byte-range task planning; Vortex interprets split ranges as
    // row positions, which is a separate limitation from equality-delete read support.
    Assumptions.abort("EqualityDeleteRowReader uses byte-range splits, unsupported by Vortex");
  }

  @TestTemplate
  @Override
  public void testPosDeletesWithDeletedColumn() {
    skipDeletedColumn();
  }

  @TestTemplate
  @Override
  public void testEqualityDeleteWithDeletedColumn() {
    skipDeletedColumn();
  }

  @TestTemplate
  @Override
  public void testMixedPosAndEqDeletesWithDeletedColumn() {
    skipDeletedColumn();
  }

  @TestTemplate
  @Override
  public void testFilterOnDeletedMetadataColumn() {
    skipDeletedColumn();
  }
}
