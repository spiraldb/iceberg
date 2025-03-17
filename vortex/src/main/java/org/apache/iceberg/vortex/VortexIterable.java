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

import dev.vortex.api.Array;
import dev.vortex.api.ArrayStream;
import dev.vortex.api.DType;
import dev.vortex.api.File;
import dev.vortex.api.ScanOptions;
import dev.vortex.impl.Files;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VortexIterable<T> extends CloseableGroup implements CloseableIterable<T> {
  private static final Logger LOG = LoggerFactory.getLogger(VortexIterable.class);

  private final InputFile inputFile;
  private final Function<DType, VortexRowReader<T>> rowReaderFunc;
  private final Function<DType, VortexBatchReader<T>> batchReaderFunction;
  private final List<String> projection;

  // TODO(aduffy): pushdown Iceberg Expression as Vortex ExprRef.

  VortexIterable(
      InputFile inputFile,
      Schema icebergSchema,
      Function<DType, VortexRowReader<T>> readerFunction,
      Function<DType, VortexBatchReader<T>> batchReaderFunction) {
    this.inputFile = inputFile;
    this.projection = Lists.transform(icebergSchema.columns(), Types.NestedField::name);
    this.rowReaderFunc = readerFunction;
    this.batchReaderFunction = batchReaderFunction;
  }

  @Override
  public CloseableIterator<T> iterator() {
    // TODO(aduffy): this HadoopInputFile assumption is maybe silly.
    File vortexFile = newVortexFile(inputFile);
    addCloseable(vortexFile);

    ArrayStream batchStream =
        vortexFile.newScan(ScanOptions.builder().addAllColumns(projection).build());
    Preconditions.checkNotNull(batchStream, "batchStream");

    if (rowReaderFunc != null) {
      VortexRowReader<T> rowFunction = rowReaderFunc.apply(batchStream.getDataType());
      return new VortexRowIterator<>(batchStream, rowFunction);
    } else {
      VortexBatchReader<T> batchTransform = batchReaderFunction.apply(batchStream.getDataType());
      CloseableIterator<Array> batchIterator = new VortexBatchIterator(batchStream);
      return CloseableIterator.transform(batchIterator, batchTransform::read);
    }
  }

  private static File newVortexFile(InputFile inputFile) {
    Preconditions.checkArgument(
        inputFile instanceof HadoopInputFile, "Vortex only supports HadoopInputFile currently");

    HadoopInputFile hadoopInputFile = (HadoopInputFile) inputFile;
    URI path = hadoopInputFile.getPath().toUri();
    switch (path.getScheme()) {
      case "s3a":
        return Files.open(path, s3PropertiesFromHadoopConf(hadoopInputFile.getConf()));
      case "file":
        return Files.open(path, Map.of());
      default:
        // TODO(aduffy): add support for Azure
        throw new IllegalArgumentException("Unsupported scheme: " + path.getScheme());
    }
  }

  static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
  static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
  static final String FS_S3A_SESSION_TOKEN = "fs.s3a.session.token";
  static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
  static final String FS_S3A_ENDPOINT_REGION = "fs.s3a.endpoint.region";
  static final String FS_S3A_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
  static final String ANONYMOUS_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";

  private static Map<String, String> s3PropertiesFromHadoopConf(Configuration hadoopConf) {
    VortexS3Properties properties = new VortexS3Properties();

    for (Map.Entry<String, String> entry : hadoopConf) {
      switch (entry.getKey()) {
        case FS_S3A_ACCESS_KEY:
          properties.setAccessKeyId(entry.getValue());
          break;
        case FS_S3A_SECRET_KEY:
          properties.setSecretAccessKey(entry.getValue());
          break;
        case FS_S3A_SESSION_TOKEN:
          properties.setSessionToken(entry.getValue());
          break;
        case FS_S3A_ENDPOINT:
          properties.setEndpoint(entry.getValue());
          break;
        case FS_S3A_ENDPOINT_REGION:
          properties.setRegion(entry.getValue());
          break;
        case FS_S3A_CREDENTIALS_PROVIDER:
          if (entry.getValue().equals(ANONYMOUS_CREDENTIALS_PROVIDER)) {
            properties.setSkipSignature(true);
          } else {
            throw new IllegalArgumentException(
                "Vortex does not support custom AWSCredentialsProvider: " + entry.getValue());
          }
          break;
        default:
          LOG.warn(
              "Ignoring unknown s3a connector property: {}={}", entry.getKey(), entry.getValue());
          break;
      }
    }

    return properties.asProperties();
  }

  static class VortexBatchIterator implements CloseableIterator<Array> {
    private ArrayStream stream;

    private VortexBatchIterator(ArrayStream stream) {
      this.stream = stream;
    }

    @Override
    public Array next() {
      return stream.getCurrent();
    }

    @Override
    public boolean hasNext() {
      return stream.next();
    }

    @Override
    public void close() {
      if (stream != null) {
        stream.close();
      }
      stream = null;
    }
  }

  static class VortexRowIterator<T> implements CloseableIterator<T> {
    private final ArrayStream stream;
    private final VortexRowReader<T> rowReader;

    private Array currentBatch = null;
    private int batchIndex = 0;
    private int batchLen = 0;

    VortexRowIterator(ArrayStream stream, VortexRowReader<T> rowReader) {
      this.stream = stream;
      this.rowReader = rowReader;
      if (stream.next()) {
        currentBatch = stream.getCurrent();
        batchLen = (int) currentBatch.getLen();
      }
    }

    @Override
    public void close() throws IOException {
      // Do not close the ArrayStream, it is closed by the parent.
      currentBatch.close();
      currentBatch = null;
    }

    @Override
    public boolean hasNext() {
      // See if we need to fill a new batch first.
      if (currentBatch == null || batchIndex == batchLen) {
        advance();
      }

      return currentBatch != null;
    }

    @Override
    public T next() {
      T nextRow = rowReader.read(currentBatch, batchIndex);
      batchIndex++;
      return nextRow;
    }

    private void advance() {
      if (stream.next()) {
        currentBatch = stream.getCurrent();
        batchIndex = 0;
        batchLen = (int) currentBatch.getLen();
      } else {
        currentBatch = null;
        batchLen = 0;
      }
    }
  }
}
