package com.example

import org.apache.spark.sql.SparkSession

object JsonToIceberg {

  def main(args: Array[String]): Unit = {
    val jsonPath   = if (args.nonEmpty) args(0) else "data/sample.json"
    val catalogUri = sys.env.getOrElse("LAKEKEEPER_URI", "http://localhost:8181/catalog")
    val warehouse  = sys.env.getOrElse("ICEBERG_WAREHOUSE", "demo")

    val master = sys.env.getOrElse("SPARK_MASTER", "local[*]")

    val spark = SparkSession.builder()
      .appName("JsonToIceberg")
      .master(master)
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      // Lakekeeper REST catalog
      .config("spark.sql.catalog.lakekeeper",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lakekeeper.catalog-impl",
        "org.apache.iceberg.rest.RESTCatalog")
      .config("spark.sql.catalog.lakekeeper.uri", catalogUri)
      .config("spark.sql.catalog.lakekeeper.warehouse", warehouse)
      .config("spark.sql.catalog.lakekeeper.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
      // MinIO (S3-compatible) via S3A
      .config("spark.hadoop.fs.s3a.endpoint",          "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key",        "minio-root-user")
      .config("spark.hadoop.fs.s3a.secret.key",        "minio-root-password")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3.impl",   "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    val df = spark.read.option("multiline", "true").json(jsonPath)
    df.printSchema()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakekeeper.default")

    df.writeTo("lakekeeper.default.events")
      .tableProperty("write.format.default", "parquet")
      .createOrReplace()

    println(s"Written ${df.count()} records to lakekeeper.default.events")
    spark.stop()
  }
}
