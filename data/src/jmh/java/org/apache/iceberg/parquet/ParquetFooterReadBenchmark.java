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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RandomInternalData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Compares Parquet's seek-based footer read ({@code ParquetFileReader.open}) with the tail read in
 * {@link ParquetIO#open}.
 *
 * <p>Each invocation opens {@value #NUM_FILES} files. {@code openFooter} only reads the footer;
 * {@code readRowGroups} also reads every row group (all column chunks, without decoding), which is
 * the IO a full scan performs. Files are about 4 MiB, above the 1 MiB eager fetch threshold.
 *
 * <p>Storage modes:
 *
 * <ul>
 *   <li>{@code simulated}: in-memory files that model S3InputStream request semantics with a fixed
 *       latency per request and a transfer rate. Runs anywhere.
 *   <li>{@code local-s3}: S3FileIO against a local S3-compatible server started with Testcontainers
 *       (the same RustFS image as the AWS integration tests' {@code ObjectStoreUtil}). Exercises
 *       the real S3InputStream and HTTP client and counts GetObject requests exactly; timings
 *       reflect localhost, not S3 latency. Requires Docker.
 *   <li>{@code s3}: S3FileIO against a real bucket. Pass {@code
 *       -Dbench.s3.location=s3://bucket/prefix} (and optionally {@code -Daws.region=...}) to the
 *       benchmark JVM; test files are written under a random prefix and deleted after each trial.
 * </ul>
 *
 * <p>To run the default modes ({@code simulated} and {@code local-s3}):
 *
 * <pre>
 * ./gradlew :iceberg-data:jmh -PjmhIncludeRegex=ParquetFooterReadBenchmark \
 *     -PjmhOutputPath=benchmark/parquet-footer-read.txt
 * </pre>
 *
 * <p>To run against a real bucket, build the JMH jar and pass the parameters to JMH directly:
 *
 * <pre>
 * ./gradlew :iceberg-data:jmhJar
 * java -jar data/build/libs/iceberg-data-*-jmh.jar ParquetFooterReadBenchmark -p storage=s3 \
 *     -jvmArgsAppend -Dbench.s3.location=s3://bucket/prefix
 * </pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 1, timeUnit = TimeUnit.HOURS)
public class ParquetFooterReadBenchmark {
  private static final int NUM_FILES = 20;
  // number of values per file; random longs, so roughly 8 bytes each (~4 MiB files)
  private static final int VALUES_PER_FILE = 500_000;
  private static final long SIMULATED_BANDWIDTH_MIBPS = 100;
  private static final String LOCAL_S3_IMAGE = "rustfs/rustfs:1.0.0";
  private static final int LOCAL_S3_PORT = 9000;
  private static final String LOCAL_S3_BUCKET = "benchmark";

  @Param({"simulated", "local-s3"})
  private String storage;

  @Param({"seek", "tail"})
  private String footerRead;

  @Param({"10", "100", "1000"})
  private int numColumns;

  @Param({"20"})
  private long simulatedLatencyMillis;

  private final AtomicLong requests = new AtomicLong();
  private final List<InputFile> files = Lists.newArrayList();
  private final List<String> s3Locations = Lists.newArrayList();
  private final ParquetReadOptions options =
      ParquetReadOptions.builder().withUseHadoopVectoredIo(true).build();
  private S3FileIO s3FileIO;
  private GenericContainer<?> localS3;
  private long invocations = 0;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Schema schema =
        new Schema(
            IntStream.rangeClosed(1, numColumns)
                .mapToObj(id -> optional(id, "col_" + id, Types.LongType.get()))
                .collect(Collectors.toList()));
    List<Record> records =
        RandomInternalData.generate(schema, VALUES_PER_FILE / numColumns, 0xF00DL);

    switch (storage) {
      case "simulated":
        for (int i = 0; i < NUM_FILES; i += 1) {
          InMemoryOutputFile out = new InMemoryOutputFile();
          write(out, schema, records);
          files.add(
              new SimulatedObjectStoreInputFile(
                  out.toByteArray(), simulatedLatencyMillis, SIMULATED_BANDWIDTH_MIBPS, requests));
        }
        break;
      case "local-s3":
        setupLocalS3(schema, records);
        break;
      case "s3":
        String location = System.getProperty("bench.s3.location");
        Preconditions.checkArgument(location != null, "Set -Dbench.s3.location=s3://bucket/prefix");
        setupS3(S3Client.builder(), location, schema, records);
        break;
      default:
        throw new IllegalArgumentException("Unknown storage: " + storage);
    }

    requests.set(0);
  }

  private void setupLocalS3(Schema schema, List<Record> records) throws IOException {
    this.localS3 =
        new GenericContainer<>(DockerImageName.parse(LOCAL_S3_IMAGE))
            .withExposedPorts(LOCAL_S3_PORT)
            .withEnv("RUSTFS_ACCESS_KEY", "admin")
            .withEnv("RUSTFS_SECRET_KEY", "password")
            .waitingFor(Wait.forHttp("/health/ready").forPort(LOCAL_S3_PORT));
    localS3.start();

    URI endpoint =
        URI.create("http://" + localS3.getHost() + ":" + localS3.getMappedPort(LOCAL_S3_PORT));
    S3ClientBuilder builder =
        S3Client.builder()
            .endpointOverride(endpoint)
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("admin", "password")));
    try (S3Client admin = builder.build()) {
      admin.createBucket(request -> request.bucket(LOCAL_S3_BUCKET));
    }

    setupS3(builder, "s3://" + LOCAL_S3_BUCKET, schema, records);
  }

  private void setupS3(
      S3ClientBuilder builder, String location, Schema schema, List<Record> records)
      throws IOException {
    AtomicLong getRequests = requests;
    ExecutionInterceptor countGets =
        new ExecutionInterceptor() {
          @Override
          public void beforeExecution(
              Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
            if (context.request() instanceof GetObjectRequest) {
              getRequests.incrementAndGet();
            }
          }
        };
    S3Client client =
        builder
            .httpClientBuilder(Apache5HttpClient.builder())
            .overrideConfiguration(config -> config.addExecutionInterceptor(countGets))
            .build();
    this.s3FileIO = new S3FileIO(() -> client);
    s3FileIO.initialize(ImmutableMap.of());

    String prefix =
        String.format(
            Locale.ROOT,
            "%s/parquet-footer-read-%s",
            location.replaceAll("/+$", ""),
            UUID.randomUUID());
    for (int i = 0; i < NUM_FILES; i += 1) {
      String path = String.format(Locale.ROOT, "%s/file-%03d.parquet", prefix, i);
      write(s3FileIO.newOutputFile(path), schema, records);
      s3Locations.add(path);
      long length = s3FileIO.newInputFile(path).getLength();
      // pass the length, as manifests do, so opening a file needs no HEAD request
      files.add(s3FileIO.newInputFile(path, length));
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    long fileOpens = invocations * NUM_FILES;
    System.out.printf(
        Locale.ROOT,
        "%n[%s footerRead=%s numColumns=%d] %d requests over %d file reads = %.2f requests per file%n",
        storage,
        footerRead,
        numColumns,
        requests.get(),
        fileOpens,
        fileOpens > 0 ? (double) requests.get() / fileOpens : 0.0);
    if (s3FileIO != null) {
      s3Locations.forEach(s3FileIO::deleteFile);
      s3FileIO.close();
    }

    if (localS3 != null) {
      localS3.stop();
    }
  }

  @Benchmark
  @Threads(1)
  public void openFooter(Blackhole blackhole) throws IOException {
    invocations += 1;
    for (InputFile file : files) {
      try (ParquetFileReader reader = open(file)) {
        blackhole.consume(reader.getFooter());
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readRowGroups(Blackhole blackhole) throws IOException {
    invocations += 1;
    for (InputFile file : files) {
      try (ParquetFileReader reader = open(file)) {
        Object rowGroup;
        while ((rowGroup = reader.readNextRowGroup()) != null) {
          blackhole.consume(rowGroup);
        }
      }
    }
  }

  private ParquetFileReader open(InputFile file) throws IOException {
    org.apache.parquet.io.InputFile parquetFile = ParquetIO.file(file);
    if ("tail".equals(footerRead)) {
      return ParquetIO.open(parquetFile, options);
    }

    return ParquetFileReader.open(parquetFile, options);
  }

  private static void write(OutputFile out, Schema schema, List<Record> records)
      throws IOException {
    try (DataWriter<StructLike> writer =
        Parquet.writeData(out)
            .schema(schema)
            .createWriterFunc(InternalWriter::createWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (Record record : records) {
        writer.write(record);
      }
    }
  }
}
