package com.example

import com.example.common.SparkSessionFactory

import java.sql.Timestamp
import java.time.LocalDateTime

object CreateNewTable {

  private val namespace = "lakekeeper.default"
  private val table = s"$namespace.orders"

  case class Order(
    order_id:    Long,
    customer_id: Long,
    status:      String,
    amount:      BigDecimal,
    created_at:  Timestamp
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionFactory.create("CreateNewTable")
    import spark.implicits._

    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")

    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $table (
        order_id    BIGINT,
        customer_id BIGINT,
        status      STRING,
        amount      DECIMAL(10, 2),
        created_at  TIMESTAMP
      )
      USING iceberg
      PARTITIONED BY (days(created_at))
    """)

    val orders = Seq(
      Order(1, 101, "completed", BigDecimal("99.90"),  Timestamp.valueOf(LocalDateTime.of(2024, 1, 15, 10,  0))),
      Order(2, 102, "pending",   BigDecimal("149.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 15, 11, 30))),
      Order(3, 103, "completed", BigDecimal("59.99"),  Timestamp.valueOf(LocalDateTime.of(2024, 1, 16,  9,  0)))
    )

    orders.toDF().writeTo(table).append()

    spark.table(table).show()

    spark.stop()
  }
}
