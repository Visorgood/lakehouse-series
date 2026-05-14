package com.example.examples

import com.example.common.Models.Order
import com.example.common.{Models, SparkSessionFactory}
import org.apache.spark.sql.functions.partitioning.days

import java.sql.Timestamp
import java.time.LocalDateTime

object E5_DistributionModes extends App {

  private val namespace       = "lakekeeper.default"
  private val sortedTableName = s"$namespace.orders_sorted"

  val spark = SparkSessionFactory.create("E5_DistributionModes")
  import spark.implicits._

  val orders1 = (1L to 350000L).map { x: Long =>
    Order(x, x, "completed", BigDecimal("99.99"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 15, 10, 0)))
  }
  val orders2 = (350001L to 700000L).map { x: Long =>
    Order(x, x, "completed", BigDecimal("99.99"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 16, 10, 0)))
  }

  spark.sql(s"DROP TABLE IF EXISTS $sortedTableName PURGE")

  spark.emptyDataset[Order].to(Models.orderSchema)
    .writeTo(sortedTableName)
    .tableProperty("format-version",                   "3")
    .tableProperty("write.format.default",             "parquet")
    .tableProperty("write.metadata.compression-codec", "none")
    .tableProperty("write.parquet.compression-codec",  "zstd")
    .partitionedBy(days($"created_at"))
    .createOrReplace()

  spark.sql(s"ALTER TABLE $sortedTableName WRITE ORDERED BY customer_id ASC NULLS LAST")

  (orders1 ++ orders2).toDS().writeTo(sortedTableName).append()

  spark.sql(s"""
    SELECT concat(split(file_path, '/')[7], '/', split(file_path, '/')[8]) as file,
           record_count as count,
           file_size_in_bytes as size,
           readable_metrics['customer_id']['lower_bound'] as lower_bound,
           readable_metrics['customer_id']['upper_bound'] as upper_bound
    FROM $sortedTableName.files
  """).show(truncate = false)

  spark.stop()
}
