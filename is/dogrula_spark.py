# -*- coding: utf-8 -*-
"""Spark'ın S3'e, Delta Lake'e ve Kafka'ya ulaşabildiğini dener.
   docker compose exec spark spark-submit is/dogrula_spark.py
İlk çalıştırmada kütüphaneler indirildiği için birkaç dakika sürebilir."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("kit-dogrulama").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.range(1000).withColumnRenamed("id", "sayi")

df.write.mode("overwrite").parquet("s3a://islenmis/_dogrulama/spark")
n = spark.read.parquet("s3a://islenmis/_dogrulama/spark").count()
print(f"✅ Spark → S3 Parquet yazdı ve okudu ({n} satır)")

df.write.format("delta").mode("overwrite").save("s3a://tablolar/_dogrulama")
spark.range(5).withColumnRenamed("id", "sayi").write.format("delta").mode("append").save("s3a://tablolar/_dogrulama")
surum = spark.sql("DESCRIBE HISTORY delta.`s3a://tablolar/_dogrulama`").count()
eski = spark.read.format("delta").option("versionAsOf", 0).load("s3a://tablolar/_dogrulama").count()
print(f"✅ Delta Lake: {surum} sürüm · zaman yolculuğu (sürüm 0) {eski} satır")

k = (spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092")
     .option("subscribe", "kit-dogrulama").option("startingOffsets", "earliest").load().count())
print(f"✅ Spark → Kafka okudu ('kit-dogrulama' konusunda {k} mesaj)")
print("🎉 Spark tarafı çalışıyor.")
spark.stop()
