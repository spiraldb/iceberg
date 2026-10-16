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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.mapping.NameMapping;
import org.junit.jupiter.api.Test;

class TestVortexNameMappings {
  @Test
  void readsMetadataOncePerFileAndLength() {
    VortexNameMappings cache = new VortexNameMappings(2);
    AtomicInteger reads = new AtomicInteger();
    Supplier<NameMapping> loader =
        () -> {
          reads.incrementAndGet();
          return NameMapping.empty();
        };
    cache.get("file", 100, loader);
    cache.get("file", 100, loader);
    assertThat(reads).hasValue(1);
    cache.get("file", 101, loader);
    assertThat(reads).hasValue(2);
  }

  @Test
  void concurrentSplitsShareOneMetadataLoad() throws Exception {
    VortexNameMappings cache = new VortexNameMappings(2);
    AtomicInteger reads = new AtomicInteger();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Supplier<NameMapping> loader =
        () -> {
          reads.incrementAndGet();
          entered.countDown();
          try {
            if (!release.await(5, TimeUnit.SECONDS)) {
              throw new IllegalStateException("Loader timed out");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
          return NameMapping.empty();
        };
    try {
      var first = executor.submit(() -> cache.get("file", 1, loader));
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
      var second = executor.submit(() -> cache.get("file", 1, loader));
      release.countDown();
      assertThat(first.get()).isSameAs(second.get());
      assertThat(reads).hasValue(1);
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void boundsTheCacheAndCachesMissingMetadata() {
    VortexNameMappings cache = new VortexNameMappings(2);
    AtomicInteger reads = new AtomicInteger();
    Supplier<NameMapping> loader =
        () -> {
          reads.incrementAndGet();
          return null;
        };
    cache.get("missing", 1, loader);
    cache.get("missing", 1, loader);
    assertThat(reads).hasValue(1);
    for (int i = 0; i < 10; i++) {
      cache.get("file" + i, 1, loader);
    }
    assertThat(cache.size()).isLessThanOrEqualTo(2);
  }
}
