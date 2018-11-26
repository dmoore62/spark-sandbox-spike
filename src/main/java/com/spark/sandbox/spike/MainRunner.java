package com.spark.sandbox.spike;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class MainRunner {
  public static void main( String[] args ) {
    System.out.println( "Running ... " );

    //Tag Name and column names will be picked up from the step meta data
    String tagName = "fieldTag";

    List<String> keyCols = new ArrayList<String>(  ){{
      add( "ID" );
    }};

    List<String> matchCols = new ArrayList<String>(  ){{
      add( "ID" );
      add( "NAME" );
    }};

    //Initializing Spark Context
    SparkRunner runner = new SparkRunner();

    //Mock RDDs of KettleRows for both the Reference and Compare Sets
    JavaRDD<KettleRow> ref = RDDUtil.getReferenceRDD( runner.getSc() );
    JavaRDD<KettleRow> comp = RDDUtil.getCompareRDD( runner.getSc() );

    //Convert KettleRows to SparkRows
    JavaRDD<Row> refRows = RDDUtil.convert( ref );
    JavaRDD<Row> compRows = RDDUtil.convert( comp );

    //Convert to Datasets
    Dataset<Row> refDs = runner.getSpark().createDataFrame( refRows, RDDUtil.getStructType() );
    Dataset<Row> compDs = runner.getSpark().createDataFrame( compRows, RDDUtil.getStructType() );

    //Debug print sets
    refDs.show();
    compDs.show();

    //Deleted rows are in the reference set and not in the compare set - tag column added
    Dataset<Row> deletedDs = refDs
        .join( compDs, JavaConverters.asScalaIteratorConverter( keyCols.iterator() ).asScala().toSeq(), "leftanti")
        .withColumn( tagName, lit("DELETED") );

    //New rows are in the compare set and not in the reference set - tag column added
    Dataset<Row> newDs = compDs
        .join( refDs, JavaConverters.asScalaIteratorConverter( keyCols.iterator() ).asScala().toSeq(), "leftanti")
        .withColumn( tagName, lit("NEW") );

    //Unchanged Rows are in both sets and matched on both key and value columns - tag column added
    Dataset<Row> unchangedDs = refDs
        .join( compDs, JavaConverters.asScalaIteratorConverter( matchCols.iterator() ).asScala().toSeq(), "inner" )
        .withColumn( tagName, lit("IDENTICAL") );

    //Union the previous 3 sets to diff against the remaining keys
    Dataset<Row> unionDs = deletedDs.union( newDs ).union( unchangedDs );

    //Changed rows are in the compare set and not in the union of the 3 other sets - tag column added
    Dataset<Row> changedDs = compDs
        .join( unionDs, JavaConverters.asScalaIteratorConverter( keyCols.iterator() ).asScala().toSeq(), "leftanti" )
        .withColumn( tagName, lit("MODIFIED") );

    //Get list of key column names and map to Spark Columns
    List<Column> columns = keyCols.stream().map( Column::new ).collect( Collectors.toList());

    //Union all sets and sort on key columns
    Dataset<Row> output = unionDs.union( changedDs ).sort( columns.toArray( new Column[columns.size()] ) );

    //Debug
    output.show();

    System.out.println( "Ended!!!" );
  }
}
