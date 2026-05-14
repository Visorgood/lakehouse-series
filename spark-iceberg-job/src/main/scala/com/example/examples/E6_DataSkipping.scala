package com.example.examples

import com.example.common.SparkSessionFactory


object E6_DataSkipping extends App {

  private val namespace = "lakekeeper.default"
  private val tableName = s"$namespace.orders_sorted"

  val spark = SparkSessionFactory.create("E6_DataSkipping")

  val res = spark.table(tableName)
    .where("created_at = to_timestamp('2024-01-16 10:00:00')")
    .where("customer_id = 400000")
    .select("order_id", "amount")

  res.explain(extended = false)
  res.show(truncate = false)

  spark.stop()
}
