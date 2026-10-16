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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.vortex.io.NativeReadable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

class TestVortexIO {
  @Test
  void defaultRangesUseOneFullRead() throws IOException {
    RangeStream stream = new RangeStream();
    try (NativeReadable readable = VortexIO.readable(file(stream))) {
      ByteBuffer target = ByteBuffer.allocateDirect(3 * 1024 * 1024 + 8);
      target.position(4).limit(target.capacity() - 4);
      int expected = target.remaining();
      readable.readFully(123, target);
      assertThat(stream.calls).isEqualTo(1);
      assertThat(stream.lastLength).isEqualTo(expected);
      assertThat(target.position()).isEqualTo(target.limit());
      assertThat(target.get(4)).isEqualTo((byte) 123);
      assertThat(target.get(target.limit() - 1)).isEqualTo((byte) (123 + expected - 1));
      assertThat(target.get(0)).isZero();
    }
  }

  @Test
  void vectoredReadsFillTheNativeSlice() throws IOException {
    RangeStream stream =
        new RangeStream() {
          @Override
          public void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate) {
            calls++;
            assertThat(ranges).hasSize(1);
            FileRange range = ranges.get(0);
            ByteBuffer buffer = allocate.apply(range.length());
            assertThat(buffer.isDirect()).isTrue();
            for (int i = 0; i < range.length(); i++) {
              buffer.put((byte) (range.offset() + i));
            }
            range.byteBuffer().complete(buffer.flip());
          }
        };
    try (NativeReadable readable = VortexIO.readable(file(stream))) {
      ByteBuffer target = ByteBuffer.allocateDirect(32);
      target.position(7).limit(15);
      readable.readFully(20, target);
      assertThat(stream.calls).isEqualTo(1);
      assertThat(target.position()).isEqualTo(15);
      assertThat(target.get(7)).isEqualTo((byte) 20);
      assertThat(target.get(14)).isEqualTo((byte) 27);
      assertThat(target.get(6)).isZero();
    }
  }

  @Test
  void vectoredReadsMayReturnAViewOfTheNativeSlice() throws IOException {
    RangeStream stream =
        new RangeStream() {
          @Override
          public void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate) {
            calls++;
            FileRange range = ranges.get(0);
            ByteBuffer buffer = allocate.apply(range.length());
            for (int i = 0; i < range.length(); i++) {
              buffer.put(i, (byte) (range.offset() + i));
            }
            range.byteBuffer().complete(buffer.slice(0, range.length()));
          }
        };
    try (NativeReadable readable = VortexIO.readable(file(stream))) {
      ByteBuffer target = ByteBuffer.allocateDirect(32);
      target.position(7).limit(15);
      readable.readFully(20, target);
      assertThat(stream.calls).isEqualTo(1);
      assertThat(target.position()).isEqualTo(15);
      assertThat(target.get(7)).isEqualTo((byte) 20);
      assertThat(target.get(14)).isEqualTo((byte) 27);
      assertThat(target.get(6)).isZero();
    }
  }

  @Test
  void sameStartAddressDetectsViewsOfTheSameMemory() {
    ByteBuffer direct = ByteBuffer.allocateDirect(32);
    direct.position(8).limit(24);
    ByteBuffer directView = direct.slice();
    assertThat(VortexIO.sameStartAddress(directView, direct)).isTrue();
    assertThat(VortexIO.sameStartAddress(direct.duplicate(), direct)).isTrue();
    assertThat(VortexIO.sameStartAddress(directView.slice(1, 8), direct)).isFalse();
    assertThat(VortexIO.sameStartAddress(ByteBuffer.allocateDirect(16), direct)).isFalse();

    ByteBuffer heap = ByteBuffer.allocate(32);
    heap.position(8);
    assertThat(VortexIO.sameStartAddress(heap.slice(), heap)).isTrue();
    assertThat(VortexIO.sameStartAddress(heap.slice(1, 8), heap)).isFalse();
    assertThat(VortexIO.sameStartAddress(ByteBuffer.allocate(24), heap)).isFalse();
    assertThat(VortexIO.sameStartAddress(directView, heap)).isFalse();
  }

  @Test
  void heapSlicesPreserveSurroundingBytes() throws IOException {
    RangeStream stream = new RangeStream();
    try (NativeReadable readable = VortexIO.readable(file(stream))) {
      ByteBuffer target = ByteBuffer.allocate(32);
      target.position(7).limit(15);
      readable.readFully(20, target);
      assertThat(stream.lastLength).isEqualTo(8);
      assertThat(target.get(7)).isEqualTo((byte) 20);
      assertThat(target.get(6)).isZero();
    }
  }

  @Test
  void rejectsOverflowingRangesBeforeOpeningStream() throws IOException {
    try (NativeReadable readable = VortexIO.readable(file(new RangeStream()))) {
      assertThatThrownBy(() -> readable.readFully(Long.MAX_VALUE, ByteBuffer.allocate(8)))
          .isInstanceOf(EOFException.class)
          .hasMessageContaining("Cannot read 8 bytes");
      assertThatThrownBy(() -> readable.readFully(-1, ByteBuffer.allocate(8)))
          .isInstanceOf(EOFException.class)
          .hasMessageContaining("Cannot read 8 bytes");
    }
  }

  private static InputFile file(SeekableInputStream stream) {
    return new InputFile() {
      @Override
      public long getLength() {
        return 10 * 1024 * 1024;
      }

      @Override
      public SeekableInputStream newStream() {
        return stream;
      }

      @Override
      public String location() {
        return "test.vortex";
      }

      @Override
      public boolean exists() {
        return true;
      }
    };
  }

  private static class RangeStream extends SeekableInputStream implements RangeReadable {
    int calls;
    int lastLength;

    @Override
    public long getPos() {
      return 0;
    }

    @Override
    public void seek(long position) {
      throw new AssertionError("seek");
    }

    @Override
    public int read() {
      throw new AssertionError("read");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) {
      calls++;
      lastLength = length;
      for (int i = 0; i < length; i++) {
        buffer[offset + i] = (byte) (position + i);
      }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) {
      throw new AssertionError("tail");
    }
  }
}
