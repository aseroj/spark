package com.crowley.spark.sparkSql;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;


public class StackoverflowSurvey {


    private static final String AGE_MIDPOINT = "age_midpoint";
    private static final String SALARY_MIDPOINT = "salary_midpoint";
    private static final String SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket";

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder().appName("stackoverflowSurvey").master("local[*]").getOrCreate();
        DataFrameReader reader = sparkSession.read();

        Dataset<Row> responses = reader.option("header", "true").csv("in/2016-stack-overflow-survey-responses.csv");

        System.out.println("stack overflow survey schema");
        responses.printSchema();

        System.out.println("Print 20 records");
        responses.show(20);

        System.out.println("Print some columns");
        responses.select(col("so_region"), col("self_identification")).show(2);

        System.out.println("filter by column");
        responses.filter(col("country").equalTo("Afghanistan")).show(10);
        responses.filter(col("country").like("%ghanistan%")).show(10);

        System.out.println("group by ...");
        RelationalGroupedDataset group = responses.groupBy("occupation");
        group.count().show();

        System.out.println("casted schema");
        Dataset<Row> castedResponses = responses
                .withColumn(SALARY_MIDPOINT, col(SALARY_MIDPOINT).cast("integer"))
                .withColumn(AGE_MIDPOINT, col(AGE_MIDPOINT).cast("integer"));

        castedResponses.printSchema();

        System.out.println("age less than 20");
        castedResponses.filter(col(AGE_MIDPOINT).$less(20)).show();

        System.out.println("order by salary midpoint");
        castedResponses.orderBy(col(SALARY_MIDPOINT).desc()).show();

        System.out.println("group by country, aggregate by avg salary and age");
        RelationalGroupedDataset datasetGroupByCountry = castedResponses.groupBy("country");
        datasetGroupByCountry.agg(avg(SALARY_MIDPOINT), max(AGE_MIDPOINT)).show();

        Dataset<Row> responseWithSalaryBucket = castedResponses.withColumn(SALARY_MIDPOINT_BUCKET, col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000));
        System.out.println("with salary bucket col");
        responseWithSalaryBucket.select(col(SALARY_MIDPOINT_BUCKET), col(SALARY_MIDPOINT)).show();

        System.out.println("group by salary bucket");
        responseWithSalaryBucket.groupBy(SALARY_MIDPOINT_BUCKET).count().orderBy(col(SALARY_MIDPOINT_BUCKET)).show();

        sparkSession.stop();
    }

}
