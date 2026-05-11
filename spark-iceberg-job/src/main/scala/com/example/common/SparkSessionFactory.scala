package com.example.common

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def create(appName: String): SparkSession = {
    val catalogUri = sys.env.getOrElse("LAKEKEEPER_URI",    "http://localhost:8181/catalog")
    val warehouse  = sys.env.getOrElse("ICEBERG_WAREHOUSE", "demo")
    val master     = sys.env.getOrElse("SPARK_MASTER",      "local[*]")
    val s3Endpoint = sys.env.getOrElse("S3_ENDPOINT",       "http://localhost:9000")
    val s3Key      = sys.env.getOrElse("S3_ACCESS_KEY",     "minio-root-user")
    val s3Secret   = sys.env.getOrElse("S3_SECRET_KEY",     "minio-root-password")

    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.lakekeeper",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lakekeeper.catalog-impl",
        "org.apache.iceberg.rest.RESTCatalog")
      .config("spark.sql.catalog.lakekeeper.uri",       catalogUri)
      .config("spark.sql.catalog.lakekeeper.warehouse", warehouse)
      .config("spark.sql.catalog.lakekeeper.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO")
      .config("spark.hadoop.fs.s3a.endpoint",          s3Endpoint)
      .config("spark.hadoop.fs.s3a.access.key",        s3Key)
      .config("spark.hadoop.fs.s3a.secret.key",        s3Secret)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3.impl",   "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()
  }
}
