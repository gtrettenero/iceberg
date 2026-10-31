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
package org.apache.iceberg.parquet;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * An in-memory file that models the request behavior of {@code S3InputStream}.
 *
 * <p>Every request pays a fixed time-to-first-byte and every byte pays a transfer cost. Sequential
 * reads open a stream at the current position on the first read; seeking backward or skipping
 * forward more than {@link #SKIP_SIZE} closes it, so the next read opens a new stream (a new
 * request). Positioned reads and tail reads are one request each.
 */
class SimulatedObjectStoreInputFile implements InputFile {
  // matches the default S3InputStream skip size
  private static final long SKIP_SIZE = 1024 * 1024;

  private final byte[] bytes;
  private final long requestLatencyNanos;
  private final double nanosPerByte;
  private final AtomicLong requests;

  SimulatedObjectStoreInputFile(
      byte[] bytes, long requestLatencyMillis, long bandwidthMiBps, AtomicLong requests) {
    this.bytes = bytes;
    this.requestLatencyNanos = TimeUnit.MILLISECONDS.toNanos(requestLatencyMillis);
    this.nanosPerByte = 1_000_000_000.0 / (bandwidthMiBps * 1024.0 * 1024.0);
    this.requests = requests;
  }

  @Override
  public long getLength() {
    return bytes.length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new SimulatedStream();
  }

  @Override
  public String location() {
    return "simulated://file";
  }

  @Override
  public boolean exists() {
    return true;
  }

  private void request() {
    requests.incrementAndGet();
    LockSupport.parkNanos(requestLatencyNanos);
  }

  private void transfer(long numBytes) {
    LockSupport.parkNanos((long) (numBytes * nanosPerByte));
  }

  private class SimulatedStream extends SeekableInputStream implements RangeReadable {
    private long pos = 0;
    // position of the open sequential stream, or -1 if no stream is open
    private long streamPos = -1;

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) {
      this.pos = newPos;
    }

    private void positionStream() {
      if (streamPos >= 0 && pos >= streamPos && pos - streamPos <= SKIP_SIZE) {
        // small forward skips read through the open stream
        transfer(pos - streamPos);
      } else {
        // no open stream, a backward seek, or a large skip: open a new stream at pos
        request();
      }

      this.streamPos = pos;
    }

    @Override
    public int read() {
      byte[] single = new byte[1];
      return read(single, 0, 1) < 0 ? -1 : single[0] & 0xFF;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) {
      if (pos >= bytes.length) {
        return -1;
      }

      positionStream();
      int toRead = (int) Math.min(length, bytes.length - pos);
      System.arraycopy(bytes, (int) pos, buffer, offset, toRead);
      transfer(toRead);
      pos += toRead;
      streamPos = pos;
      return toRead;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) {
      request();
      System.arraycopy(bytes, (int) position, buffer, offset, length);
      transfer(length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) {
      request();
      int toRead = Math.min(length, bytes.length);
      System.arraycopy(bytes, bytes.length - toRead, buffer, offset, toRead);
      transfer(toRead);
      return toRead;
    }

    @Override
    public void close() {
      this.streamPos = -1;
    }
  }
}
