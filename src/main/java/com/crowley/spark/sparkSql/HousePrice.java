package com.crowley.spark.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class HousePrice {

//    read the input source data RealEstate.csv
//    groupBy location, agg avg price per SQ ft & max price, sort by avg price per SQ ft
//    -----------------
//    Column headers
//    MLS,Location,Price,Bedrooms,Bathrooms,Size,Price SQ Ft,Status

    private static final String LOCATION = "Location";
    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FT = "Price SQ Ft";

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder().appName("HousePrice").master("local[1]").getOrCreate();
        DataFrameReader reader = sparkSession.read();
        Dataset<Row> responses = reader.option("header", "true").csv("in/RealEstate.csv");

        Dataset<Row> castedResponses = responses
                .withColumn(PRICE, col(PRICE).cast("long"))
                .withColumn(PRICE_SQ_FT, col(PRICE_SQ_FT).cast("long"));

        System.out.println("group by Location, aggregate by avg price and max price, ordered by avg price SQ Ft");
        castedResponses.groupBy(LOCATION)
                .agg(avg(PRICE_SQ_FT), max(PRICE))
                .orderBy(col("avg(" + PRICE_SQ_FT + ")").desc())
                .show();

        sparkSession.stop();
    }

}
