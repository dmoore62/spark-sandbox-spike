package com.spark.sandbox.spike;

import java.io.Serializable;

/**
 * This is just a mock KettleRow
 */
public class KettleRow implements Serializable {
  private static final String[] HEADERS = {"ID", "NAME"};
  private String[] headers;
  private Object[] columns;

  public KettleRow( Object[] vals ) {
    this.headers = HEADERS;
    this.columns = vals;
  }

  public String[] getHeaders() {
    return headers;
  }

  public void setHeaders( String[] headers ) {
    this.headers = headers;
  }

  public Object[] getColumns() {
    return columns;
  }

  public void setColumns( Object[] columns ) {
    this.columns = columns;
  }
}
