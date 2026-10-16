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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Bounded LRU cache of Vortex file name mappings, keyed by file location and length. */
final class VortexNameMappings {
  private record Key(String location, long length) {}

  private final long maximumSize;
  private final Map<Key, NameMapping> mappings = new LinkedHashMap<>(16, 0.75f, true);
  private final Map<Key, CompletableFuture<NameMapping>> pending = Maps.newHashMap();

  VortexNameMappings(long maximumSize) {
    this.maximumSize = maximumSize;
  }

  NameMapping get(String location, long length, Supplier<NameMapping> loader) {
    Key key = new Key(location, length);
    CompletableFuture<NameMapping> future;
    boolean load;
    synchronized (this) {
      NameMapping cached = mappings.get(key);
      if (cached != null || mappings.containsKey(key)) {
        return cached;
      }
      future = pending.get(key);
      load = future == null;
      if (load) {
        future = new CompletableFuture<>();
        pending.put(key, future);
      }
    }
    if (load) {
      try {
        NameMapping value = loader.get();
        synchronized (this) {
          mappings.put(key, value);
          while (mappings.size() > maximumSize) {
            mappings.remove(mappings.keySet().iterator().next());
          }
          pending.remove(key);
        }
        future.complete(value);
      } catch (RuntimeException | Error e) {
        synchronized (this) {
          pending.remove(key);
        }
        future.completeExceptionally(e);
        throw e;
      }
    }
    return future.join();
  }

  synchronized long size() {
    return mappings.size();
  }
}
