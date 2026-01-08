/*
 * Copyright 2025 Inyo Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inyo.ducklake.ingestor;

import com.inyo.ducklake.connect.DucklakeConnectionFactory;
import com.inyo.ducklake.connect.DucklakeSinkConfig;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration test for DucklakeWriter performance using a real Postgres-backed DuckLake catalog and
 * MinIO for object storage. Based on EndToEndIntegrationTest conventions.
 */
@Testcontainers
class DucklakeWriterIntegrationTest {

  private static final Network NETWORK = Network.newNetwork();

  @Container
  @SuppressWarnings("resource")
  public static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17")
          .withNetwork(NETWORK)
          .withNetworkAliases("postgres")
          .withDatabaseName("ducklake_catalog")
          .withUsername("duck")
          .withPassword("duck");

  @Container
  public static final MinIOContainer MINIO =
      new MinIOContainer("minio/minio:latest")
          .withNetwork(NETWORK)
          .withNetworkAliases("minio")
          .withEnv("MINIO_ROOT_USER", "minio")
          .withEnv("MINIO_ROOT_PASSWORD", "minio123")
          .withCommand("server", "/data", "--console-address", ":9001");

  private DucklakeConnectionFactory factory;

  @BeforeEach
  void beforeEach() throws Exception {
    // Build config pointing Ducklake catalog to the Postgres container and S3 to MinIO
    var cfgMap = new java.util.HashMap<String, String>();
    String pgHost = POSTGRES.getHost();
    Integer pgPort = POSTGRES.getMappedPort(5432);
    cfgMap.put(
        DucklakeSinkConfig.DUCKLAKE_CATALOG_URI,
        String.format(
            "postgres:dbname=ducklake_catalog host=%s port=%d user=duck password=duck",
            pgHost, pgPort));

    // S3 config (MinIO mapped port)
    String minioHost = MINIO.getHost();
    Integer minioPort = MINIO.getMappedPort(9000);
    cfgMap.put("ducklake.data_path", "s3://test-bucket/");
    cfgMap.put("s3.url_style", "path");
    cfgMap.put("s3.use_ssl", "false");
    cfgMap.put("s3.endpoint", String.format("%s:%d", minioHost, minioPort));
    cfgMap.put("s3.access_key_id", "minio");
    cfgMap.put("s3.secret_access_key", "minio123");

    var config = new DucklakeSinkConfig(DucklakeSinkConfig.CONFIG_DEF, cfgMap);
    factory = new DucklakeConnectionFactory(config);
    factory.create();

    // Ensure bucket exists via MinIO client (MinIO Container exposes S3 API)
    try (var minioClient =
        io.minio.MinioClient.builder()
            .endpoint(String.format("http://%s:%d", minioHost, minioPort))
            .credentials("minio", "minio123")
            .build()) {
      var exists =
          minioClient.bucketExists(
              io.minio.BucketExistsArgs.builder().bucket("test-bucket").build());
      if (!exists) {
        minioClient.makeBucket(io.minio.MakeBucketArgs.builder().bucket("test-bucket").build());
      }
    }
  }

  @AfterEach
  void afterEach() {
    if (factory != null) {
      factory.close();
    }
  }

  @AfterAll
  static void afterAll() {
    if (NETWORK != null) {
      NETWORK.close();
    }
  }

  private static Schema getSchema() {
    var fId = new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null);
    var fName = new Field("name", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
    var fTimestamp =
        new Field(
            "created_at",
            new FieldType(
                false,
                new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null),
                null),
            null);
    return new Schema(List.of(fId, fName, fTimestamp));
  }

  private VectorSchemaRoot makeRoot(int[] ids, String[] names) {
    var allocator = new RootAllocator();
    var schema = getSchema();
    var root = VectorSchemaRoot.create(schema, allocator);
    var idVec = (IntVector) root.getVector("id");
    var nameVec = (VarCharVector) root.getVector("name");
    var tsVec = (TimeStampMilliVector) root.getVector("created_at");
    idVec.allocateNew(ids.length);
    nameVec.allocateNew();
    tsVec.allocateNew(ids.length);

    var timestampMillis = Timestamp.valueOf(LocalDateTime.now()).getTime();
    for (var i = 0; i < ids.length; i++) {
      idVec.setSafe(i, ids[i]);
      var bytes = names[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
      nameVec.setSafe(i, bytes, 0, bytes.length);
      tsVec.setSafe(i, timestampMillis);
    }
    root.setRowCount(ids.length);
    return root;
  }

  private void sqlExec(org.duckdb.DuckDBConnection conn, String sql) throws SQLException {
    try (var st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  // Counts how many ids from the provided array already exist in lake.main.<table>
  private int countConflicts(org.duckdb.DuckDBConnection conn, String table, int[] ids)
      throws SQLException {
    if (ids.length == 0) return 0;
    var sb = new StringBuilder();
    sb.append("SELECT COUNT(*) FROM lake.main.")
        .append(SqlIdentifierUtil.quote(table))
        .append(" WHERE id IN (");
    for (var i = 0; i < ids.length; i++) {
      if (i > 0) sb.append(',');
      sb.append('?');
    }
    sb.append(')');
    var sql = sb.toString();
    try (var ps = conn.prepareStatement(sql)) {
      for (var i = 0; i < ids.length; i++) {
        ps.setInt(i + 1, ids[i]);
      }
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private int[] generateBatchWithConflictPercent(
      int percent, int baseCount, int batchSize, int iter) {
    var ids = new int[batchSize];
    var conflicts = Math.max(0, (int) ((long) batchSize * percent / 100));
    for (var i = 0; i < conflicts; i++) {
      ids[i] = (i % baseCount) + 1;
    }
    var startId = baseCount + iter * batchSize * 10 + 1;
    for (var i = conflicts; i < batchSize; i++) {
      ids[i] = startId + (i - conflicts);
    }
    return ids;
  }

  private int tableCount(org.duckdb.DuckDBConnection conn, String table) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT COUNT(*) FROM lake.main." + SqlIdentifierUtil.quote(table))) {
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private void populateBaseline(
      org.duckdb.DuckDBConnection conn, String table, int count, int batchSize)
      throws SQLException {
    sqlExec(
        conn,
        "CREATE TABLE IF NOT EXISTS lake.main."
            + SqlIdentifierUtil.quote(table)
            + " (id INTEGER, name VARCHAR, created_at TIMESTAMP)");
    var batch = batchSize;
    var inserted = 0;
    while (inserted < count) {
      var toInsert = Math.min(batch, count - inserted);
      var ids = new int[toInsert];
      var names = new String[toInsert];
      for (var i = 0; i < toInsert; i++) {
        ids[i] = inserted + i + 1;
        names[i] = "base_" + ids[i];
      }
      var cfg =
          new com.inyo.ducklake.ingestor.DucklakeWriterConfig(
              table, true, new String[] {}, new String[] {});
      try (var writer =
              new com.inyo.ducklake.ingestor.DucklakeWriter(factory.getConnection(), cfg);
          var root = makeRoot(ids, names)) {
        writer.write(root);
      }
      inserted += toInsert;
    }
  }

  @Test
  @DisplayName("MERGE vs INSERT integration: conflict rates 1%,10%,30%")
  void integrationConflictRateComparison() throws Exception {
    var baseCount = 100_000;
    var batchSize = 10_000;
    var iterations = 2; // keep smaller for integration runtime
    var percents = new int[] {1, 10, 30};

    for (var pct : percents) {
      var tableMerge = "it_perf_merge_" + pct + "p";
      var tableInsert = "it_perf_insert_" + pct + "p";

      try (var conn = factory.getConnection()) {
        // cleanup + create tables
        sqlExec(conn, "DROP TABLE IF EXISTS lake.main." + SqlIdentifierUtil.quote(tableMerge));
        sqlExec(conn, "DROP TABLE IF EXISTS lake.main." + SqlIdentifierUtil.quote(tableInsert));
        sqlExec(
            conn,
            "CREATE TABLE lake.main."
                + SqlIdentifierUtil.quote(tableMerge)
                + " (id INTEGER, name VARCHAR, created_at TIMESTAMP)");
        sqlExec(
            conn,
            "CREATE TABLE lake.main."
                + SqlIdentifierUtil.quote(tableInsert)
                + " (id INTEGER, name VARCHAR, created_at TIMESTAMP)");

        System.out.printf(
            "%n=== Integration: conflict rate %d%% (tables: %s / %s) ===%n",
            pct, tableMerge, tableInsert);

        populateBaseline(conn, tableMerge, baseCount, batchSize);
        populateBaseline(conn, tableInsert, baseCount, batchSize);

        var cfgMerge =
            new com.inyo.ducklake.ingestor.DucklakeWriterConfig(
                tableMerge, true, new String[] {"id"}, new String[] {});
        var cfgInsert =
            new com.inyo.ducklake.ingestor.DucklakeWriterConfig(
                tableInsert, true, new String[] {}, new String[] {});

        try (var writerMerge = new com.inyo.ducklake.ingestor.DucklakeWriter(conn, cfgMerge);
            var writerInsert = new com.inyo.ducklake.ingestor.DucklakeWriter(conn, cfgInsert)) {

          var mergeTimes = new ArrayList<Long>();
          var insertTimes = new ArrayList<Long>();

          for (var iter = 0; iter < iterations; iter++) {
            var ids = generateBatchWithConflictPercent(pct, baseCount, batchSize, iter);
            var names = new String[batchSize];
            for (var i = 0; i < batchSize; i++) names[i] = pct + "_m_" + iter + "_" + i;

            var conflicts = countConflicts(conn, tableMerge, ids);
            System.out.printf(
                "pct %d%% iter %d: conflicts = %d / %d%n", pct, iter, conflicts, ids.length);

            try (var root = makeRoot(ids, names)) {
              var t0 = System.nanoTime();
              writerMerge.write(root);
              var t1 = System.nanoTime();
              mergeTimes.add(t1 - t0);
            }

            var ids2 = new int[batchSize];
            var startId = baseCount + pct * 1_000_000 + iter * batchSize + 1;
            for (var i = 0; i < batchSize; i++) ids2[i] = startId + i;
            var names2 = new String[batchSize];
            for (var i = 0; i < batchSize; i++) names2[i] = pct + "_i_" + iter + "_" + i;
            try (var root2 = makeRoot(ids2, names2)) {
              var t0 = System.nanoTime();
              writerInsert.write(root2);
              var t1 = System.nanoTime();
              insertTimes.add(t1 - t0);
            }

            System.out.printf(
                "pct %d%% iter %d: MERGE %d ms, INSERT %d ms%n",
                pct, iter, mergeTimes.get(iter) / 1_000_000, insertTimes.get(iter) / 1_000_000);
          }

          var totalMerge = mergeTimes.stream().mapToLong(Long::longValue).sum();
          var totalInsert = insertTimes.stream().mapToLong(Long::longValue).sum();
          System.out.printf(
              "pct %d%% summary: total MERGE %d ms, total INSERT %d ms%n",
              pct, totalMerge / 1_000_000, totalInsert / 1_000_000);
        }
      }
    }
  }

  @Test
  @DisplayName("MERGE vs INSERT integration: extended stats (CSV output)")
  void integrationConflictRateStats() throws Exception {
    var baseCount = 100_000;
    var batchSize = 10_000;
    var iterations = 10; // increased repetitions for statistics
    var percents = new int[] {1, 10, 30};

    var outDir = new java.io.File("build/reports/integrationTest");
    outDir.mkdirs();
    var outFile = new java.io.File(outDir, "ducklake-writer-perf.csv");
    try (var fw = new java.io.FileWriter(outFile)) {
      // CSV header
      fw.write("scenario,pct,iter,conflicts,merge_ms,insert_ms\n");

      for (var pct : percents) {
        var tableMerge = "it_perf_stats_merge_" + pct + "p";
        var tableInsert = "it_perf_stats_insert_" + pct + "p";

        try (var conn = factory.getConnection()) {
          // cleanup + create tables
          sqlExec(conn, "DROP TABLE IF EXISTS lake.main." + SqlIdentifierUtil.quote(tableMerge));
          sqlExec(conn, "DROP TABLE IF EXISTS lake.main." + SqlIdentifierUtil.quote(tableInsert));
          sqlExec(
              conn,
              "CREATE TABLE lake.main."
                  + SqlIdentifierUtil.quote(tableMerge)
                  + " (id INTEGER, name VARCHAR, created_at TIMESTAMP)");
          sqlExec(
              conn,
              "CREATE TABLE lake.main."
                  + SqlIdentifierUtil.quote(tableInsert)
                  + " (id INTEGER, name VARCHAR, created_at TIMESTAMP)");

          System.out.printf(
              "%n=== Integration stats: conflict rate %d%% (tables: %s / %s) ===%n",
              pct, tableMerge, tableInsert);

          populateBaseline(conn, tableMerge, baseCount, batchSize);
          populateBaseline(conn, tableInsert, baseCount, batchSize);

          var cfgMerge =
              new com.inyo.ducklake.ingestor.DucklakeWriterConfig(
                  tableMerge, true, new String[] {"id"}, new String[] {});
          var cfgInsert =
              new com.inyo.ducklake.ingestor.DucklakeWriterConfig(
                  tableInsert, true, new String[] {}, new String[] {});

          try (var writerMerge = new com.inyo.ducklake.ingestor.DucklakeWriter(conn, cfgMerge);
              var writerInsert = new com.inyo.ducklake.ingestor.DucklakeWriter(conn, cfgInsert)) {

            var mergeTimes = new ArrayList<Long>();
            var insertTimes = new ArrayList<Long>();

            for (var iter = 0; iter < iterations; iter++) {
              var ids = generateBatchWithConflictPercent(pct, baseCount, batchSize, iter);
              var names = new String[batchSize];
              for (var i = 0; i < batchSize; i++) names[i] = pct + "_m_" + iter + "_" + i;

              var conflicts = countConflicts(conn, tableMerge, ids);
              System.out.printf(
                  "pct %d%% iter %d: conflicts = %d / %d%n", pct, iter, conflicts, ids.length);

              long mergeMs;
              try (var root = makeRoot(ids, names)) {
                var t0 = System.nanoTime();
                writerMerge.write(root);
                var t1 = System.nanoTime();
                mergeMs = (t1 - t0) / 1_000_000L;
                mergeTimes.add(t1 - t0);
              }

              var ids2 = new int[batchSize];
              var startId = baseCount + pct * 1_000_000 + iter * batchSize + 1;
              for (var i = 0; i < batchSize; i++) ids2[i] = startId + i;
              var names2 = new String[batchSize];
              for (var i = 0; i < batchSize; i++) names2[i] = pct + "_i_" + iter + "_" + i;

              long insertMs;
              try (var root2 = makeRoot(ids2, names2)) {
                var t0 = System.nanoTime();
                writerInsert.write(root2);
                var t1 = System.nanoTime();
                insertMs = (t1 - t0) / 1_000_000L;
                insertTimes.add(t1 - t0);
              }

              // write CSV row (use US locale to ensure '.' decimal separator)
              fw.write(
                  java.lang.String.format(
                      java.util.Locale.US,
                      "stats,%d,%d,%d,%d,%d%n",
                      pct,
                      iter,
                      conflicts,
                      mergeMs,
                      insertMs));

              System.out.printf(
                  "pct %d%% iter %d: MERGE %d ms, INSERT %d ms%n", pct, iter, mergeMs, insertMs);
            }

            // compute summary stats (ms)
            double mergeMean =
                mergeTimes.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
            double insertMean =
                insertTimes.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
            double mergeStd = computeStdDev(mergeTimes);
            double insertStd = computeStdDev(insertTimes);

            fw.write(
                java.lang.String.format(
                    java.util.Locale.US,
                    "summary,%d,mean,%.3f,%.3f%n",
                    pct,
                    mergeMean,
                    insertMean));
            fw.write(
                java.lang.String.format(
                    java.util.Locale.US, "summary,%d,std,%.3f,%.3f%n", pct, mergeStd, insertStd));
            fw.flush();

            System.out.printf(
                "pct %d%% summary: mean MERGE %.3f ms (std %.3f), mean INSERT %.3f ms (std %.3f)%n",
                pct, mergeMean, mergeStd, insertMean, insertStd);
          }
        }
      }
    }
  }

  // helper to compute std dev from nanosecond times in the list; returns ms
  private static double computeStdDev(List<Long> nanos) {
    if (nanos.isEmpty()) return 0.0;
    var n = nanos.size();
    double mean = nanos.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
    double sumsq = 0.0;
    for (var v : nanos) {
      var ms = v / 1_000_000.0;
      var d = ms - mean;
      sumsq += d * d;
    }
    return Math.sqrt(sumsq / n);
  }
}
