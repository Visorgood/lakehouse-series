package com.example.common

import org.apache.spark.sql.types._

import java.sql.Timestamp

object Models {

  val orderTableName = "lakekeeper.default.orders"

  case class Order(
    order_id:    Long,
    customer_id: Long,
    status:      String,
    amount:      BigDecimal,
    created_at:  Timestamp
  )

  case class OrderStatus(order_id: Long, status: String)

  val orderSchema: StructType = StructType(Seq(
    StructField("order_id",    LongType),
    StructField("customer_id", LongType),
    StructField("status",      StringType),
    StructField("amount",      DecimalType(10, 2)),
    StructField("created_at",  TimestampType)
  ))
}
