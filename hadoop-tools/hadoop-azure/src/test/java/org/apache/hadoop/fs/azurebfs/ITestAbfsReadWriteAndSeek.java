/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.fs.azurebfs.constants.AbfsOperations;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.utils.Listener;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.testHeader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MAX_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;

/**
 * Test read, write and seek.
 * Uses package-private methods in AbfsConfiguration, which is why it is in
 * this package.
 */
@RunWith(Parameterized.class)
public class ITestAbfsReadWriteAndSeek extends AbstractAbfsScaleTest {
  private static final Path TEST_PATH = new Path("/testfile");

  @Parameterized.Parameters(name = "Size={0}")
  public static Iterable<Object[]> sizes() {
    return Arrays.asList(new Object[][]{{MIN_BUFFER_SIZE},
        {DEFAULT_READ_BUFFER_SIZE},
        {APPENDBLOB_MAX_WRITE_BUFFER_SIZE},
        {MAX_BUFFER_SIZE}});
  }

  private final int size;

  public ITestAbfsReadWriteAndSeek(final int size) throws Exception {
    this.size = size;
  }

  @Test
  public void testReadAndWriteWithDifferentBufferSizesAndSeek() throws Exception {
    testReadWriteAndSeek(size);
  }

  @Test
  public void testReadAheadRequestID() throws java.io.IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    int bufferSize = 32;
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b = new byte[bufferSize * 10];
    new Random().nextBytes(b);
    try (FSDataOutputStream stream = fs.create(TEST_PATH)) {
      stream.write(b);
    }

    final byte[] readBuffer = new byte[4 * bufferSize];
    int result;
    try (FSDataInputStream inputStream = fs.open(TEST_PATH)) {
      result = inputStream.read(readBuffer, 0, bufferSize * 4);
    }
  }

  @Test
  public void testCorrelationStreamID() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    int bufferSize = 32;
    abfsConfiguration.setReadBufferSize(bufferSize);
    final byte[] b = new byte[bufferSize * 10];
    new Random().nextBytes(b);
    try (FSDataOutputStream stream = fs.create(TEST_PATH)) {
      stream.write(b);
    }
    AbfsConfiguration conf = fs.getAbfsStore().getAbfsConfiguration();
    TracingContext tracingContext =
        new TracingContext(conf.getClientCorrelationID(),
            fs.getFileSystemID(), AbfsOperations.READ, true,
        conf.getTracingContextFormat(), null);
    AbfsInputStream inputStream =
        fs.getAbfsStore().openFileForRead(TEST_PATH, null, tracingContext);
    inputStream.registerListener(new testHeader(inputStream.getStreamID(),
        tracingContext.toString()));
    int n = inputStream.read();
  }

  private void testReadWriteAndSeek(int bufferSize) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b = new byte[2 * bufferSize];
    new Random().nextBytes(b);

    try (FSDataOutputStream stream = fs.create(TEST_PATH)) {
      stream.write(b);
    }

    final byte[] readBuffer = new byte[2 * bufferSize];
    int result;
    try (FSDataInputStream inputStream = fs.open(TEST_PATH)) {
      inputStream.seek(bufferSize);
      result = inputStream.read(readBuffer, bufferSize, bufferSize);
      assertNotEquals(-1, result);
      inputStream.seek(0);
      result = inputStream.read(readBuffer, 0, bufferSize);
    }
    assertNotEquals("data read in final read()", -1, result);
    assertArrayEquals(readBuffer, b);
  }
}
