package org.apache.iceberg.spark;

import static org.apache.iceberg.types.Types.NestedField.optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class VortexSmokeTest {
  private static final String AWS_ACCESS_KEY = System.getenv("AWS_ACCESS_KEY");
  private static final String AWS_SECRET_KEY = System.getenv("AWS_SECRET_KEY");
  private static final Configuration CONF = new Configuration();
  private static final String WAREHOUSE = "s3a://vortex-iceberg-dev/warehouse";

  private static final Map<String, LocalDate> FILES_TO_PARTITION_VALUES;
  private static final Map<String, Long> RECORD_COUNTS;

  private static final Schema CITIBIKE_SCHEMA =
      new Schema(
          optional(1, "ride_id", Types.StringType.get()),
          optional(2, "rideable_type", Types.StringType.get()),
          optional(3, "started_at", Types.TimestampType.withoutZone()),
          optional(4, "ended_at", Types.TimestampType.withoutZone()),
          optional(5, "start_station_name", Types.StringType.get()),
          optional(6, "start_station_id", Types.StringType.get()),
          optional(7, "end_station_name", Types.StringType.get()),
          optional(8, "end_station_id", Types.StringType.get()),
          optional(9, "start_lat", Types.DoubleType.get()),
          optional(10, "start_lng", Types.DoubleType.get()),
          optional(11, "end_lat", Types.DoubleType.get()),
          optional(12, "end_lng", Types.DoubleType.get()),
          optional(13, "member_casual", Types.StringType.get()));

  static {
    // Set HadoopConfig used for the HadoopCatalog.
    CONF.set("fs.s3a.access.key", AWS_ACCESS_KEY);
    CONF.set("fs.s3a.secret.key", AWS_SECRET_KEY);

    FILES_TO_PARTITION_VALUES = Maps.newHashMap();
    RECORD_COUNTS = Maps.newHashMap();

    RECORD_COUNTS.put("202409-citibike-tripdata_1.vortex", 1000000L);
    RECORD_COUNTS.put("202409-citibike-tripdata_2.vortex", 1000000L);
    RECORD_COUNTS.put("202409-citibike-tripdata_3.vortex", 1000000L);
    RECORD_COUNTS.put("202409-citibike-tripdata_4.vortex", 1000000L);
    RECORD_COUNTS.put("202409-citibike-tripdata_5.vortex", 997898L);
    RECORD_COUNTS.put("202410-citibike-tripdata_1.vortex", 1000000L);
    RECORD_COUNTS.put("202410-citibike-tripdata_2.vortex", 1000000L);
    RECORD_COUNTS.put("202410-citibike-tripdata_3.vortex", 1000000L);
    RECORD_COUNTS.put("202410-citibike-tripdata_4.vortex", 1000000L);
    RECORD_COUNTS.put("202410-citibike-tripdata_5.vortex", 1000000L);
    RECORD_COUNTS.put("202410-citibike-tripdata_6.vortex", 150054L);
    RECORD_COUNTS.put("202411-citibike-tripdata_1.vortex", 1000000L);
    RECORD_COUNTS.put("202411-citibike-tripdata_2.vortex", 1000000L);
    RECORD_COUNTS.put("202411-citibike-tripdata_3.vortex", 1000000L);
    RECORD_COUNTS.put("202411-citibike-tripdata_4.vortex", 710134L);
    RECORD_COUNTS.put("202412-citibike-tripdata_1.vortex", 1000000L);
    RECORD_COUNTS.put("202412-citibike-tripdata_2.vortex", 1000000L);
    RECORD_COUNTS.put("202412-citibike-tripdata_3.vortex", 311171L);
    RECORD_COUNTS.put("202501-citibike-tripdata_1.vortex", 1000000L);
    RECORD_COUNTS.put("202501-citibike-tripdata_2.vortex", 1000000L);
    RECORD_COUNTS.put("202501-citibike-tripdata_3.vortex", 124475L);
    RECORD_COUNTS.put("202502-citibike-tripdata_1.vortex", 1000000L);
    RECORD_COUNTS.put("202502-citibike-tripdata_2.vortex", 1000000L);
    RECORD_COUNTS.put("202502-citibike-tripdata_3.vortex", 31257L);

    FILES_TO_PARTITION_VALUES.put(
        "202409-citibike-tripdata_1.vortex", LocalDate.parse("2024-09-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202409-citibike-tripdata_2.vortex", LocalDate.parse("2024-09-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202409-citibike-tripdata_3.vortex", LocalDate.parse("2024-09-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202409-citibike-tripdata_4.vortex", LocalDate.parse("2024-09-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202409-citibike-tripdata_5.vortex", LocalDate.parse("2024-09-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202410-citibike-tripdata_1.vortex", LocalDate.parse("2024-10-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202410-citibike-tripdata_2.vortex", LocalDate.parse("2024-10-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202410-citibike-tripdata_3.vortex", LocalDate.parse("2024-10-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202410-citibike-tripdata_4.vortex", LocalDate.parse("2024-10-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202410-citibike-tripdata_5.vortex", LocalDate.parse("2024-10-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202410-citibike-tripdata_6.vortex", LocalDate.parse("2024-10-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202411-citibike-tripdata_1.vortex", LocalDate.parse("2024-11-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202411-citibike-tripdata_2.vortex", LocalDate.parse("2024-11-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202411-citibike-tripdata_3.vortex", LocalDate.parse("2024-11-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202411-citibike-tripdata_4.vortex", LocalDate.parse("2024-11-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202412-citibike-tripdata_1.vortex", LocalDate.parse("2024-12-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202412-citibike-tripdata_2.vortex", LocalDate.parse("2024-12-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202412-citibike-tripdata_3.vortex", LocalDate.parse("2024-12-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202501-citibike-tripdata_1.vortex", LocalDate.parse("2025-01-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202501-citibike-tripdata_2.vortex", LocalDate.parse("2025-01-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202501-citibike-tripdata_3.vortex", LocalDate.parse("2025-01-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202502-citibike-tripdata_1.vortex", LocalDate.parse("2025-02-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202502-citibike-tripdata_2.vortex", LocalDate.parse("2025-02-01"));
    FILES_TO_PARTITION_VALUES.put(
        "202502-citibike-tripdata_3.vortex", LocalDate.parse("2025-02-01"));
  }

  // Step 1: create the warehouse, populate it with Citibike data.
  @Test
  public void setupWarehouse() throws IOException, ClassNotFoundException, URISyntaxException {
    URI baseUri = new URI("s3a://vortex-iceberg-dev/warehouse/db/trips/data/");

    // Create the table
    try (HadoopCatalog catalog = new HadoopCatalog(CONF, WAREHOUSE)) {
      TableIdentifier tableId = TableIdentifier.of("db", "trips");
      PartitionSpec spec = PartitionSpec.builderFor(CITIBIKE_SCHEMA).month("started_at").build();
      // Create a new table, with a partition space on started_at time
      Table table = catalog.createTable(tableId, CITIBIKE_SCHEMA, spec);
      List<DataFile> appendFiles = Lists.newArrayList();

      for (Map.Entry<String, LocalDate> entry : FILES_TO_PARTITION_VALUES.entrySet()) {
        String fileName = entry.getKey();
        LocalDate partitionDate = entry.getValue();
        int months = DateTimeUtil.daysToMonths(DateTimeUtil.daysFromDate(partitionDate));

        Path path = new Path(baseUri.resolve(fileName));

        DataFile dataFile =
            DataFiles.builder(spec)
                .withInputFile(HadoopInputFile.fromLocation(path.toString(), CONF))
                .withRecordCount(RECORD_COUNTS.get(fileName))
                .withFormat(FileFormat.VORTEX)
                .withPartitionValues(List.of(String.valueOf(months)))
                .build();
        appendFiles.add(dataFile);
      }

      // Make a new APPEND transaction.
      AppendFiles txn = table.newAppend();
      appendFiles.forEach(txn::appendFile);
      txn.commit();

      System.out.println("committed new manifest list!");
    }
  }

  // Run some Spark SQL queries against the warehouse (using partition pruning!)
  @Test
  public void scanWarehouse() {
    try (SparkSession spark = newSparkSession("scanWarehouse")) {
      spark.sql("select count(*) from db.trips").show();

      // Do with filtering
      Dataset<Row> df =
          spark.sql(
              "select count(*) from db.trips where started_at BETWEEN '2024-12-1 00:00:00' AND '2024-12-31 23:59:59'");
      df.explain(true);
      long start = System.nanoTime();
      System.err.println("Begin query");
      spark.sparkContext().setLogLevel("DEBUG");
      df.show();
      long duration = System.nanoTime() - start;
      System.err.println("single partition query: " + duration + " nanos");
    }
  }

  private static SparkSession newSparkSession(String name) {
    return SparkSession.builder()
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        // use Spark iceberg catalog
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        // use hadoop type
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        // set the warehouse path
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
        .config("spark.sql.defaultCatalog", "iceberg")
        .appName(name)
        .master("local")
        .getOrCreate();
  }
}
