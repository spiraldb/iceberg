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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public final class VortexSparkEte {
  private static final Schema EMPLOYEE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "name", Types.StringType.get()),
          required(3, "salary", Types.LongType.get()));

  private static Table vortexTable;

  @TempDir private static File tempDir;

  @BeforeAll
  public static void beforeAll() {
    Tables tables = new HadoopTables();
    File employeesDir = new File(tempDir, "employees");
    String employeesLocation = employeesDir.getAbsolutePath();
    vortexTable = tables.create(EMPLOYEE_SCHEMA, employeesLocation);

    // Add a new file for the table.

    // Import the file from the classpath into the table's data directory.
    File dataDir = new File(employeesDir, "data");
    dataDir.mkdirs();

    Path newFilePath = new File(dataDir, "1.vortex").toPath();
    loadToPath("employees.vortex", newFilePath);

    // Append the data file to this table
    try (FileIO io = vortexTable.io()) {
      InputFile inputFile = io.newInputFile(newFilePath.toAbsolutePath().toString());
      DataFile newDataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withInputFile(inputFile)
              .withRecordCount(4)
              .build();

      AppendFiles append = vortexTable.newAppend();
      append.appendFile(newDataFile);
      append.commit();

      // Make sure that the table metadata shows one snapshot with 1 file
      // and 4 records.
      assertThat(vortexTable.currentSnapshot().addedDataFiles(io)).hasSize(1);
      assertThat(
              Iterables.getOnlyElement(vortexTable.currentSnapshot().addedDataFiles(io))
                  .recordCount())
          .isEqualTo(4);
    }
  }

  @AfterAll
  public static void afterAll() {
    // Delete the table and all of its data/metadata.
    if (vortexTable != null) {
      new HadoopTables().dropTable(vortexTable.location());
    }
  }

  @Test
  public void testBasic() {
    SparkSession spark = SparkSession.builder().master("local").appName("testBasic").getOrCreate();

    System.out.println("LOADING FROM TABLE: " + vortexTable.location());
    Dataset<Row> employees = spark.read().format("iceberg").load(vortexTable.location());
    assertThat(employees.count()).isEqualTo(4);

    employees.printSchema();
    employees.show();
    //    employees.select("name").show();
    //    assertThat(employees.where("id > 1").count()).isEqualTo(3);
  }

  private static void loadToPath(String resourcePath, Path outputPath) {
    try (InputStream inputStream = VortexSparkEte.class.getResourceAsStream(resourcePath);
        OutputStream outputStream = Files.newOutputStream(outputPath)) {
      Preconditions.checkNotNull(inputStream, "Cannot find resource: " + resourcePath);

      ByteStreams.copy(inputStream, outputStream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load resource: " + resourcePath, e);
    }
  }
}
