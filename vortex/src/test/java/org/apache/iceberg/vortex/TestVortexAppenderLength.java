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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.vortex.api.VortexWriter;
import dev.vortex.io.NativeWritable;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.MetricsConfig;
import org.junit.jupiter.api.Test;

class TestVortexAppenderLength {
  @Test
  void lengthTracksNativeOutputAndPendingBytes() {
    VortexWriter writer = mock(VortexWriter.class);
    when(writer.bytesWritten()).thenReturn(1024L, 2048L);
    when(writer.bufferedBytes()).thenReturn(512L, 0L);
    VortexFileAppender<Object> appender =
        new VortexFileAppender<>(
            writer,
            (value, root, index) -> {},
            new Schema(List.of()),
            VortexArrowBridge.arrowAllocator(),
            1024,
            mock(NativeWritable.class),
            new org.apache.iceberg.Schema(),
            MetricsConfig.getDefault(),
            1024);
    assertThat(appender.length()).isEqualTo(1536);
    assertThat(appender.length()).isEqualTo(2048);
  }
}
