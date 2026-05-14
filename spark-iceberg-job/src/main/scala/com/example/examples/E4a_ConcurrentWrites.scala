package com.example.examples

import com.example.common.Models.{Order, orderTableName}
import com.example.common.{SparkSessionFactory, TableUtils}

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object E4a_ConcurrentWrites extends App {

  val spark = SparkSessionFactory.create("E4a_ConcurrentWrites")
  import spark.implicits._

  implicit val ec: ExecutionContext = ExecutionContext.global

  println("=== Before concurrent appends ===")
  TableUtils.showTable(spark, orderTableName)

  spark.sql(s"ALTER TABLE $orderTableName SET TBLPROPERTIES ('write.isolation-level' = 'snapshot')")

  def append(rows: Seq[Order]): Unit = {
    val id = Thread.currentThread().getId
    println(s"Thread $id: starting append")
    rows.toDS().writeTo(orderTableName).append()
    println(s"Thread $id: append completed")
  }

  val data = Seq(
    Seq(Order(10L, 201L, "pending", BigDecimal("50.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 20, 12, 0)))),
    Seq(Order(11L, 202L, "pending", BigDecimal("90.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 20, 13, 0)))),
    Seq(Order(12L, 203L, "pending", BigDecimal("75.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 20, 14, 0))))
  )

  val futures = data.map(batch => Future(append(batch)))
  Await.result(Future.sequence(futures), Duration.Inf)

  println("=== After concurrent appends ===")
  TableUtils.showTable(spark, orderTableName)

  println("=== All appends completed — each got its own snapshot ===")
  TableUtils.showTableSnapshots(spark, orderTableName)

  spark.stop()
}
