package com.example.examples

import com.example.common.Models.{Order, OrderStatus, orderTableName}
import com.example.common.{SparkSessionFactory, TableUtils}

import java.sql.Timestamp
import java.time.LocalDateTime

object E2_MergeInto extends App {

  val spark = SparkSessionFactory.create("E2_MergeInto")
  import spark.implicits._

  println("=== Before MERGE INTO ===")
  TableUtils.showTable(spark, orderTableName)

  val newData = Seq(
    Order(2L, 102L, "completed", BigDecimal("149.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 15, 11, 30))),
    Order(6L, 106L, "pending", BigDecimal("87.15"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 18, 7, 15))),
  ).toDS()

  newData.createOrReplaceTempView("newData")

  spark.sql(s"""
    MERGE INTO $orderTableName t
    USING newData s ON t.order_id = s.order_id
    WHEN MATCHED               THEN UPDATE SET t.status = s.status
    WHEN NOT MATCHED BY TARGET THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND t.created_at < to_timestamp('2024-01-15') THEN DELETE
  """)

  println("=== After MERGE INTO ===")
  TableUtils.showTable(spark, orderTableName)

  TableUtils.showTableSnapshots(spark, orderTableName)

  spark.stop()
}
