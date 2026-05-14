package com.example.examples

import com.example.common.Models.{Order, orderTableName}
import com.example.common.{SparkSessionFactory, TableUtils}

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object E4b_ConcurrentWrites extends App {

  val spark = SparkSessionFactory.create("E4b_ConcurrentWrites")
  import spark.implicits._

  implicit val ec: ExecutionContext = ExecutionContext.global

  println("=== Before concurrent writes ===")
  TableUtils.showTable(spark, orderTableName)

  spark.sql(s"ALTER TABLE $orderTableName SET TBLPROPERTIES ('write.isolation-level' = 'serializable')")

  def mergeInto(rows: Seq[Order]): Unit = {
    val id = Thread.currentThread().getId
    println(s"Thread $id: starting merge")
    rows.toDS().createOrReplaceTempView(s"incoming_$id")
    spark.sql(s"""
      MERGE INTO $orderTableName t
      USING incoming_$id s ON t.order_id = s.order_id
      WHEN MATCHED     THEN UPDATE SET t.status = s.status
      WHEN NOT MATCHED THEN INSERT *
    """)
    println(s"Thread $id: merge completed")
  }

  val data = Seq(
    Seq(Order(10L, 201L, "completed", BigDecimal("50.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 20, 12, 0)))),
    Seq(Order(11L, 202L, "completed", BigDecimal("90.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 20, 13, 0)))),
    Seq(Order(12L, 203L, "completed", BigDecimal("75.00"), Timestamp.valueOf(LocalDateTime.of(2024, 1, 20, 14, 0))))
  )

  val futures = data.map(batch => Future(mergeInto(batch)).recover {
    case ex => println(s"Merge failed (expected under serializable isolation): ${ex.getMessage}")
  })
  Await.result(Future.sequence(futures), Duration.Inf)

  println("=== After concurrent writes ===")
  TableUtils.showTable(spark, orderTableName)

  println("=== Only one merge completed ===")
  TableUtils.showTableSnapshots(spark, orderTableName)

  spark.stop()
}
