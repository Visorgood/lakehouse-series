# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Repository purpose

Companion code for the [Lakehouse series blog posts](https://viacheslavinozemtsev.io/blog-posts). Each runnable example maps to a concept covered in a post.

## Architecture

Two modules work together:

**`iceberg-rest-catalog/`** — local Lakehouse infrastructure as Docker Compose:
- **Lakekeeper** (`:8181`) — Iceberg REST Catalog backed by PostgreSQL. Exposes both the Iceberg Catalog API (used by Spark and other engines) and a Management API for warehouse admin.
- **MinIO** (`:9000` S3 API, `:9001` console) — object storage where Iceberg data and metadata files land, in the `iceberg` bucket under the `warehouse/` key prefix.
- **PostgreSQL** — Lakekeeper's backing store for catalog metadata (warehouse registry, namespace/table pointers).
- One-shot init containers: `migrate` (runs DB migrations), `bootstrap` (accepts ToU), `initialwarehouse` (creates the `demo` warehouse via Management API), `createbuckets` (creates the `iceberg` bucket in MinIO).

**`spark-iceberg-job/`** — Scala/sbt project (Spark 4.0, Iceberg 1.10.1, Scala 2.13):
- `common/SparkSessionFactory` — single entry point for all jobs that helps create Spark sessions. Reads all connection config from env vars (see below). Always use this; never construct `SparkSession` directly.
- Job objects (e.g. `CreateNewTable`) each have a `main` method and are run individually via `sbt runMain`.

All Iceberg tables use the three-part name `lakekeeper.default.<table>` — `lakekeeper` is the catalog name registered in `SparkSessionFactory`, `default` is the namespace.

## Local stack

```bash
cd iceberg-rest-catalog
docker compose up -d          # start all components
docker compose down -v        # full reset (wipes db-data and minio-data volumes)
```

**After a full reset**, the `initialwarehouse` container re-creates the `demo` warehouse automatically. If it fails silently, create it manually:

```bash
curl -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" \
  -d @iceberg-rest-catalog/create-warehouse.json
```

Verify the warehouse exists before running any Spark job:

```bash
curl -s http://localhost:8181/management/v1/warehouse | python3 -m json.tool
```

## Building and running jobs

```bash
cd spark-iceberg-job
sbt assembly            # fat jar created in the directory target/scala-2.13/spark-iceberg-job-assembly-0.1.0.jar
sbt "runMain com.example.CreateNewTable"
```

Heap is configured in `.sbtopts` (4 gb max) — required for `assembly`.

## SparkSessionFactory env vars

| Variable | Default | Notes |
|---|---|---|
| `LAKEKEEPER_URI` | `http://localhost:8181/catalog` | Iceberg REST Catalog endpoint |
| `ICEBERG_WAREHOUSE` | `demo` | Must match the warehouse registered in Lakekeeper |
| `SPARK_MASTER` | `local[*]` | |
| `S3_ENDPOINT` | `http://localhost:9000` | MinIO S3 API |
| `S3_ACCESS_KEY` | `minio-root-user` | |
| `S3_SECRET_KEY` | `minio-root-password` | |

## Key files

- `iceberg-rest-catalog/create-warehouse.json` — warehouse definition sent to Lakekeeper on init. S3 credential fields must be `aws-access-key-id` / `aws-secret-access-key`.
- `spark-iceberg-job/build.sbt` — dependency versions (pin `sparkVersion` and `icebergVersion` here).
- `spark-iceberg-job/.sbtopts` — JVM flags for sbt; increase `-Xmx` if assembly OOMs on a new machine.
