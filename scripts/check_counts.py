from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Check') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate()
print(f"Bronze Count: {spark.read.format('delta').load('/data/delta/bronze').count()}")
print(f"Silver Count: {spark.read.format('delta').load('/data/delta/silver').count()}")
