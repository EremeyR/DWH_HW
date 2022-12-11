# -*- coding: utf-8 -*-

import operator
import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

user = "eremzin"
price_path = "/user/{}/data/data3/rosstat/price/*".format(user)
products_for_stat_path = "/user/{}/data/data3/rosstat/products_for_stat.csv".format(user)
product_path = "/user/{}/data/data3/rosstat/product.csv".format(user)
city_path = "/user/{}/data/data3/rosstat/city.csv".format(user)

priceSchema = (
    StructType()
    .add("city_id", StringType())
    .add("product_id", IntegerType())
    .add("price", StringType())
)

citySchema = ( 
    StructType()
    .add("city",StringType())
    .add("city_id",IntegerType()) 
)

okDemSchema = ( 
    StructType()
    .add("city",StringType())
    .add("user_cnt",IntegerType())
    .add("age_avg",FloatType())
    .add("men_cnt",IntegerType())
    .add("women_cnt",IntegerType())
    .add("men_share",FloatType())
    .add("women_share",FloatType())
)

productSchema = ( 
    StructType()
    .add("product",StringType())
    .add("product_id",IntegerType()) 
)

productForStatSchema = StructType().add("product_id",IntegerType())

def main(argv):
    input_path = argv[1]
    output_path = argv[2]

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

    cityDF = ( 
        spark.read
        .option("header", "false")
        .option("sep", ";")
        .schema(citySchema)
        .csv(city_path)
    )

    productForStatDF = ( 
        spark.read
        .option("header", "false")
        .schema(productForStatSchema)
        .csv(products_for_stat_path)
    )

    productDF = ( 
        spark.read
        .option("header", "false")
        .option("sep", ";")
        .schema(productSchema)
        .csv(product_path)
    )

    okDemDF = ( 
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .schema(okDemSchema)
        .csv(input_path)
    )

    requirements = (
        okDemDF
        .agg(
            sf.round(sf.max("age_avg"), 2).alias("max_avg_age"),
            sf.min("age_avg").alias("min_avg_age"),
            sf.max("men_share").alias("max_men_share"),
            sf.max("women_share").alias("max_women_share")
        )
    ).collect()

    maxAvgAge = requirements[0][0]
    mixAvgAge = requirements[0][1]
    maxMenShare = requirements[0][2]
    maxWomenShare = requirements[0][3]

    eps = 0.001 # константа для сравнения вещественных чисел

    selectedCitiesDF = (
        okDemDF
        .select("city")
        .filter((sf.abs(sf.round(sf.col("age_avg"), 2) - maxAvgAge) < eps) 
                | (sf.abs(sf.round(sf.col("age_avg"), 2) - mixAvgAge) < eps)
                | (sf.abs(sf.round(sf.col("men_share"), 2) - maxMenShare) < eps)
                | (sf.abs(sf.round(sf.col("women_share"), 2) - maxWomenShare) < eps)
                )
        .join(sf.broadcast(cityDF), okDemDF.city == cityDF.city, how = "inner")
        .select (
            cityDF.city,
            "city_id"
        )
    )

    cityPriceDF = (
        priceDF
        .join(sf.broadcast(selectedCitiesDF), selectedCitiesDF.city_id == priceDF.city_id, how = "inner")
        .join(productDF, priceDF.product_id == productDF.product_id, how = "inner")
        .select(sf.col("city").alias("city_name"),
                selectedCitiesDF.city_id,
                "product",
                "price"
        )
    )

    productPriceStatDF = (
        cityPriceDF
        .groupBy("city_id")
        .agg(sf.min("price").alias("cheapest_product_price"),
             sf.max("price").alias("most_expensive_product_price")
            )
        .withColumn("price_difference", sf.col("most_expensive_product_price") - sf.col("cheapest_product_price"))
    )

    productStatDF = (
        cityPriceDF
        .join(sf.broadcast(productPriceStatDF), cityPriceDF.city_id == productPriceStatDF.city_id)
        .withColumn("cheapest_product_name", 
                    sf.when(sf.col("price") == sf.col("cheapest_product_price"), sf.col("product"))
                    .otherwise(None)
                   )
        .withColumn("most_expensive_product_name", 
                    sf.when(sf.col("price") == sf.col("most_expensive_product_price"), sf.col("product"))
                    .otherwise(None)
                   )
        .select("city_name",
                "cheapest_product_name",
                "most_expensive_product_name",
                "price_difference"
        )
        .groupBy("city_name")
        .agg(
            sf.max(sf.col("cheapest_product_name")).alias("cheapest_product_name"),
            sf.max(sf.col("most_expensive_product_name")).alias("most_expensive_product_name"),
            sf.max(sf.col("price_difference")).alias("price_difference")
        )
    )

    (productStatDF
    .repartition(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("sep", ";")
    .csv(output_path)
    )

if __name__ == '__main__':
    sys.exit(main(sys.argv))
