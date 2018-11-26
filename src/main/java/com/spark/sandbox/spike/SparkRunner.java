package com.spark.sandbox.spike;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Set's up the spark context and session
 */
public class SparkRunner {
  private JavaSparkContext sc;
  private SparkSession spark;

  public SparkRunner() {
    SparkConf conf = new SparkConf(  )
        .setMaster( "local" )
        .setAppName( this.getClass().getSimpleName() );

    this.spark = SparkSession.builder().config( conf ).getOrCreate();
    this.sc = new JavaSparkContext( spark.sparkContext() );
  }

  public JavaSparkContext getSc() {
    return sc;
  }

  public void setSc( JavaSparkContext sc ) {
    this.sc = sc;
  }

  public SparkSession getSpark() {
    return spark;
  }

  public void setSpark( SparkSession spark ) {
    this.spark = spark;
  }
}
