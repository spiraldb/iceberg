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
 * Exercises the Spark columnar read path for Vortex tables with position deletes.
 *
 * <p>Vortex applies position deletes and residual filters natively in the scan (see {@code
 * BaseBatchReader.newPushdownBatchIterable}), so the position-delete cases inherited from {@link
 * TestSparkReaderDeletes} run unchanged. The equality-delete and {@code _deleted}-with-delete-files
 * cases are skipped: the native pushdown path intentionally does not perform the post-scan
 * processing those require.
 */
public class TestSparkVortexReaderDeletes extends TestSparkReaderDeletes {

  @Parameters(name = "fileFormat = {0}, formatVersion = {1}, vectorized = {2}, planningMode = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.VORTEX, 2, true, PlanningMode.DISTRIBUTED},
      new Object[] {FileFormat.VORTEX, 3, true, PlanningMode.LOCAL},
    };
  }

  // Position deletes are dropped inside the Vortex scan, so they never reach Spark and are not
  // reflected in the NumDeletes metric. Disable delete-count assertions for this path.
  @Override
  protected boolean countDeletes() {
    return false;
  }

  private static void skipUnsupported() {
    Assumptions.abort(
        "Vortex columnar reads apply position deletes and filters via native scan pushdown; "
            + "equality deletes and the _deleted metadata column with delete files are not "
            + "supported on this path");
  }

  // --- equality-delete cases inherited from DeleteReadTests ---

  @TestTemplate
  @Override
  public void testEqualityDeletes() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testEqualityDateDeletes() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testEqualityDeletesWithRequiredEqColumn() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testEqualityDeletesSpanningMultipleDataFiles() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testMixedPositionAndEqualityDeletes() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testMultipleEqualityDeleteSchemas() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testEqualityDeleteByNull() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testEqualityDeleteBinaryColumn() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testEqualityDeleteStructColumn() {
    skipUnsupported();
  }

  // --- equality-delete and _deleted cases from TestSparkReaderDeletes ---

  @TestTemplate
  @Override
  public void testEqualityDeleteWithFilter() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testReadEqualityDeleteRows() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testPosDeletesWithDeletedColumn() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testEqualityDeleteWithDeletedColumn() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testMixedPosAndEqDeletesWithDeletedColumn() {
    skipUnsupported();
  }

  @TestTemplate
  @Override
  public void testFilterOnDeletedMetadataColumn() {
    skipUnsupported();
  }
}
