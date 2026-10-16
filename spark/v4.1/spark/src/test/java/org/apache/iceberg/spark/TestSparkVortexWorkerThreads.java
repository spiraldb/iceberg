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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

class TestSparkVortexWorkerThreads extends TestBaseWithCatalog {
  @BeforeEach
  void createVortexTable() {
    sql(
        "CREATE TABLE %s (id BIGINT) USING iceberg TBLPROPERTIES ('write.format.default'='vortex')",
        tableName);
    sql("INSERT INTO %s VALUES (1)", tableName);
  }

  @AfterEach
  void dropVortexTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void tableSettingReachesBatchReader() {
    setWorkerThreads("-1");
    assertThatThrownBy(() -> spark.table(tableName).collectAsList())
        .hasStackTraceContaining("Invalid Vortex worker threads: -1");
  }

  @TestTemplate
  void tableSettingReachesRowReader() {
    setWorkerThreads("-1");
    assertThatThrownBy(() -> spark.table(tableName + ".changes").collectAsList())
        .hasStackTraceContaining("Invalid Vortex worker threads: -1");
  }

  @TestTemplate
  void validTableSettingReads() {
    setWorkerThreads("8");
    assertEquals("rows", ImmutableList.of(row(1L)), sql("SELECT id FROM %s", tableName));
  }

  private void setWorkerThreads(String value) {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='%s')",
        tableName, TableProperties.READ_VORTEX_WORKER_THREADS, value);
  }
}
