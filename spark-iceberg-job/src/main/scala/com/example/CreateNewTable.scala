package com.example

import com.example.common.Models.{Order, orderTableName}
import com.example.common.{Models, SparkSessionFactory, TableUtils}
import org.apache.spark.sql.functions.partitioning.days

import java.sql.Timestamp
import java.time.LocalDateTime

object CreateNewTable extends App {

  val spark = SparkSessionFactory.create("CreateNewTable")

  import spark.implicits._

  spark.sql(s"CREATE NAMESPACE IF NOT EXISTS lakekeeper.default")
  spark.sql(s"DROP TABLE IF EXISTS $orderTableName PURGE")

  val newData = Seq(
    Order(0L, 100L, "completed", BigDecimal("15.20"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 14, 3, 25))),
    Order(1L, 101L, "completed", BigDecimal("99.90"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 15, 10, 0))),
    Order(2L, 102L, "pending", BigDecimal("149.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 15, 11, 30))),
    Order(3L, 103L, "completed", BigDecimal("59.99"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 16, 9, 0)))
  )
  newData.toDS.to(Models.orderSchema)
    .writeTo(orderTableName)
    .tableProperty("format-version", "3")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("write.metadata.compression-codec", "none")
    .tableProperty("write.parquet.compression-codec", "zstd")
    .partitionedBy(days($"created_at"))
    .createOrReplace()

  spark.table(orderTableName).show()

  TableUtils.showTableSnapshots(spark, orderTableName)

  spark.stop()
}
