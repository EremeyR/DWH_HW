# -*- coding: utf-8 -*-

import operator
import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

user = "eremzin"
price_path = "/user/{}/data/data3/rosstat/price/*".format(user)
products_for_stat_path = "/user/{}/data/data3/rosstat/products_for_stat.csv".format(user)

priceSchema = (
    StructType()
    .add("city_id", StringType())
    .add("product_id", IntegerType())
    .add("price", StringType())
)

productForStatSchema = StructType().add("product_id",IntegerType())

def main(argv):
    output_path = argv[1]

    spark = SparkSession.builder.getOrCreate()

    priceDF = ( 
    spark.read
        .option("header", "false")
        .option("sep", ";")
        .schema(priceSchema)
        .csv(price_path)
    )

    priceDF = (
        priceDF.dropna()
        .select(
            sf.col("city_id"),
            sf.col("product_id"),
            sf.regexp_replace(sf.col("price"), ",", ".").cast(FloatType()).alias("price")
        )
    )

    productForStatDF = ( 
        spark.read
        .option("header", "false")
        .schema(productForStatSchema)
        .csv(products_for_stat_path)
    )

    priceStatDF = (
        productForStatDF
        .join(priceDF, productForStatDF.product_id == priceDF.product_id, how='inner')
        .select(
            productForStatDF.product_id,
            sf.col("price")
        )
        .groupBy(sf.col("product_id"))
        .agg(sf.min("price").alias("min_price"),
             sf.max("price").alias("max_price"),
             sf.round(sf.avg("price"), 2).alias("price_avg"))
    )

    (priceStatDF
    .repartition(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("sep", ";")
    .csv(output_path)
    )


if __name__ == '__main__':
    sys.exit(main(sys.argv))
