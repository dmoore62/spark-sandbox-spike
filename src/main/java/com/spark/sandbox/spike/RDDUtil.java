package com.spark.sandbox.spike;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class RDDUtil {
  /**
   * Creates Reference RDD
   * @param sc
   * @return
   */
  public static JavaRDD<KettleRow> getReferenceRDD( JavaSparkContext sc ) {
    return sc.parallelize( new ArrayList<KettleRow>(  ){{
      add( new KettleRow( new Object[]{ 1, "one" } ) );
      add( new KettleRow( new Object[]{ 2, "two" } ) );
      add( new KettleRow( new Object[]{ 3, "three" } ) );
    }} );
  }

  /**
   * Creates Comparable RDD
   * @param sc
   * @return
   */
  public static JavaRDD<KettleRow> getCompareRDD( JavaSparkContext sc ) {
    return sc.parallelize( new ArrayList<KettleRow>(  ){{
      add( new KettleRow( new Object[]{ 2, "TWO" } ) );
      add( new KettleRow( new Object[]{ 3, "three" } ) );
      add( new KettleRow( new Object[]{ 4, "four" } ) );
    }} );
  }

  /**
   * Converts KettleRow to SparkRow
   * @param rdd
   * @return
   */
  public static JavaRDD<Row> convert( JavaRDD<KettleRow> rdd ) {
    return rdd.map( row -> RowFactory.create( row.getColumns() ) );
  }

  /**
   * Simply get the column headers
   * @return
   */
  public static StructType getStructType() {
    KettleRow mockRow = new KettleRow( new Object[]{ "any", "any" } );
    List<StructField> fields = new ArrayList<>(  );

    for( String field : mockRow.getHeaders() ) {
      fields.add( DataTypes.createStructField( field, (field.equalsIgnoreCase( "id" )) ? DataTypes.IntegerType : DataTypes.StringType, false ) );
    }

    return DataTypes.createStructType( fields );
  }
}
