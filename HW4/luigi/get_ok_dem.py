# -*- coding: utf-8 -*-

import operator
import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

user = "eremzin"
city_path = "/user/{}/data/data3/rosstat/city.csv".format(user)
rs_city_path = "/user/{}/data/data3/ok/geography/rs_city.csv".format(user)
demography_path = "/user/{}/data/data3/ok/coreDemography".format(user)
country_path = "/user/{}/data/data3/ok/geography/countries.csv".format(user)
price_path = "/user/{}/data/data3/rosstat/price/*".format(user)
current_dt = "2022-10-26"

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

rawCoreDemographySchema = ( 
    StructType()
    .add("user_id",IntegerType())
    .add("create_date",StringType())
    .add("birth_date",IntegerType())
    .add("gender",IntegerType())
    .add("id_country",StringType())
    .add("id_city",IntegerType())
    .add("login_region",IntegerType())
)

countrySchema = ( 
    StructType()
    .add("id",StringType())
    .add("name",StringType())
)

priceStatSchema = ( 
    StructType()
    .add("product_id",IntegerType())
    .add("min_price",FloatType())
    .add("max_price",FloatType())
    .add("price_avg",FloatType())
)

rsCitySchema = ( 
    StructType()
    .add("ok_city_id",IntegerType())
    .add("rs_city_id",IntegerType()) 
)

def main(argv):
    input_paths = argv[1]
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

    rawCoreDemographyDF = ( 
        spark.read
        .option("header", "false")
        .option("sep", "\t")
        .schema(rawCoreDemographySchema)
        .csv(demography_path)
    )

    countryDF = ( 
        spark.read
        .option("header", "false")
        .option("sep", ",")
        .schema(countrySchema)
        .csv(country_path)
    )

    RussiaID = (
        countryDF
        .select("id")
        .where(sf.col("name") == "Россия")
    ).collect()[0][0]

    coreDemographyDF = (
        rawCoreDemographyDF
        .select(
            sf.col("user_id"),
            (sf.col("birth_date") * (24 * 60 * 60)).alias("birth_date"), # перевод из дней в секунды (unix time) 
            sf.col("gender"),
            sf.col("id_city"),
            sf.col("login_region"),
        )
        .where(sf.col("id_country") == RussiaID)
    )

    rsCityDF = ( 
        spark.read
        .option("header", "false")
        .option("sep", "\t")
        .schema(rsCitySchema)
        .csv(rs_city_path)
    ) 

    priceStatDF = ( 
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .schema(priceStatSchema)
        .csv(input_paths)
    )

    expensiveCityDF = (
        priceDF
        .groupBy(sf.col("city_id"), sf.col("product_id"))
        .agg(sf.avg("price").alias("local_price_avg"))
        .join(priceStatDF, priceStatDF.product_id == priceDF.product_id, how='inner')
        .select(
            sf.col("city_id")
        )
            .where(sf.col("local_price_avg") > sf.col("price_avg"))
            .distinct()
    )

    okDemDF = (
        coreDemographyDF
        .join(sf.broadcast(rsCityDF), coreDemographyDF.id_city == rsCityDF.ok_city_id, how = "inner")
        .join(sf.broadcast(cityDF), rsCityDF.rs_city_id == cityDF.city_id)
        .select(
            sf.col("city"),
            (sf.months_between(sf.to_date(sf.lit(current_dt)), sf.from_unixtime("birth_date"))/12)
            .cast(IntegerType()).alias('age'),
            (sf.col("gender") == 1).alias('man'),
            (sf.col("gender") == 2).alias('woman')
        )
        .groupBy("city")
        .agg(sf.count("man").alias("user_cnt"),
             sf.round(sf.avg("age"), 2).alias("age_avg"),
             sf.sum(sf.col("man").cast(IntegerType())).alias("men_cnt"),
             sf.sum(sf.col("woman").cast(IntegerType())).alias("women_cnt")
        )
        .withColumn("men_share", sf.round((sf.col("men_cnt") / sf.col("user_cnt")), 2))
        .withColumn("women_share", sf.round((1 - sf.col("men_share")), 2))
        .orderBy(sf.col("user_cnt").desc())
    )

    (okDemDF
    .repartition(1)
    .sortWithinPartitions(sf.col("user_cnt").desc())
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("sep", ";")
    .csv(output_path)
    )

if __name__ == '__main__':
    sys.exit(main(sys.argv))
