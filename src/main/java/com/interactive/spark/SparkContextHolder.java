package com.interactive.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkContextHolder {

    private static volatile SparkContextHolder SPARK_INSTANCE = null;

    private JavaSparkContext jsc;
    private SQLContext jsqlContext;
    private SparkConf sc;

    SparkSession sparkSession;


    public static SparkContextHolder getInstance() {
        if (SPARK_INSTANCE == null) {
            return getInstance(null);
        }
        return SPARK_INSTANCE;
    }

    public static SparkContextHolder getInstance(SparkConf sparkConf) {
        if (SPARK_INSTANCE == null) {
            synchronized (SparkContextHolder.class) {
                if (SPARK_INSTANCE == null) {
                    SparkContextHolder sparkContextHolder = new SparkContextHolder();
                    sparkContextHolder.initialize(sparkConf);
                    SPARK_INSTANCE = sparkContextHolder;
                }
            }

        }
        return SPARK_INSTANCE;
    }

    private SparkContextHolder() {

    }

    private void initialize(SparkConf sparkConf) {
        if (sparkConf == null) {
            sc = new SparkConf();

        } else {
            sc = sparkConf;
        }

        sc.setMaster("local");
        sc.setAppName("TEST Application");
        sc.set("spark.sql.codegen", "true");

        jsc = new JavaSparkContext(sc);
        sparkSession = SparkSession.builder().enableHiveSupport().sparkContext(jsc.sc()).getOrCreate();
        jsqlContext = sparkSession.sqlContext();

    }

    public SparkConf getConf() {
        return sc;
    }

    public SQLContext getSqlContext() {
        return jsqlContext;
    }

    public JavaSparkContext getContext() {
        return jsc;
    }
    public SparkSession getSparkSession() {
        return sparkSession;
    }
}
