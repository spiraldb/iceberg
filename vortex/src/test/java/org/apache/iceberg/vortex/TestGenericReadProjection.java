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

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.TestReadProjection;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestGenericReadProjection extends TestReadProjection {
  @Override
  protected Record writeAndRead(String desc, Schema writeSchema, Schema readSchema, Record record)
      throws IOException {
    File file = new File(tempDir, "junit-" + desc + "-" + System.nanoTime() + ".vortex");
    OutputFile outputFile = Files.localOutput(file);

    try (FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(writeSchema)
            .content(FileContent.DATA)
            .build()) {
      appender.add(record);
    }

    try (CloseableIterable<Record> records =
        formatModel().readBuilder(outputFile.toInputFile()).project(readSchema).build()) {
      return Iterables.getOnlyElement(records);
    }
  }

  // Projection binds columns by name (see GenericVortexReader and VortexSchemaWithTypeVisitor) and
  // the scan drops columns absent from the file (VortexIterable), so reordered, subset, missing,
  // nested-struct, list, and map projections all work. The tests left disabled below each hit a
  // *renamed* column (testListOfStructsProjection and testMapOfStructsProjection only in their
  // trailing rename sub-case): rebinding a renamed column to its old physical column requires
  // Iceberg field ids stored in the file, and Vortex still drops Arrow field and schema metadata on
  // write (re-verified against 0.86.1). Vortex 0.86.1 does add a file-level metadata channel
  // (VortexWriter.Builder#metadata / NativeFiles#readMetadata) that could carry the Iceberg schema
  // and give readers field ids; until reads resolve columns by id rather than by name, name-based
  // binding cannot recover a rename.

  @Test
  @Override
  @Disabled(
      "Rename resolution needs Iceberg field ids in the file, but Vortex drops Arrow metadata, so "
          + "a renamed column cannot be bound to its old physical column by name.")
  public void testRename() {}

  @Test
  @Override
  @Disabled(
      "Rename resolution needs Iceberg field ids in the file, but Vortex drops Arrow metadata, so "
          + "a renamed column cannot be bound to its old physical column by name.")
  public void testRenamedAddedField() {}

  @Test
  @Override
  @Disabled(
      "List-of-structs projection works by name, but the trailing y->z rename sub-case needs "
          + "Iceberg field ids the Vortex file does not carry, so the renamed element field reads "
          + "null.")
  public void testListOfStructsProjection() {}

  @Test
  @Override
  @Disabled(
      "Map-of-structs projection works by name, but the trailing lat->latitude rename sub-case "
          + "needs Iceberg field ids the Vortex file does not carry, so the renamed value field "
          + "cannot be bound.")
  public void testMapOfStructsProjection() {}

  private static VortexFormatModel<Record, StructType, VortexRowReader<?>> formatModel() {
    return VortexFormatModel.create(
        Record.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> GenericVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
  }
}
