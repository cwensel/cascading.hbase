/*
 * Copyright (c) 2009 Concurrent, Inc.
 * 
 * This work has been released into the public domain by the copyright holder.
 * This applies worldwide.
 * 
 * In case this is not legally possible: The copyright holder grants any entity
 * the right to use this work for any purpose, without any conditions, unless
 * such conditions are required by law.
 */

package cascading.hbase;

import java.io.IOException;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.util.Util;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HBaseScheme class is a {@link Scheme} subclass. It is used in conjunction
 * with the {@HBaseTap} to allow for the reading and writing of data to and from
 * a HBase cluster.
 * 
 * @see HBaseTap
 */
public class HBaseScheme extends Scheme {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger(HBaseScheme.class);

  /** String columns, for keeping the current column names */
  private String columns = "";

  /**
   * Default contructor
   */
  public HBaseScheme(){} 

  /**
   * 
   */
  public HBaseScheme( Fields keyFields, Fields[] valueFields ){}
  
  /**
   * Constructor for creating a HBaseScheme.
   * If multiple columns are needed they need to be put into this string
   * and separated by a space.
   */
  public HBaseScheme(String columnNames) {
    columns = columnNames;
  }


  /**
   * Method to set the column names
   * 
   * @param columnNames to be set to
   */
  public void setColumnNames(String columnNames) {
    columns = columnNames;
  }

  /**
   * Method getFamilyNames returns the columnNames.
   * 
   * @return the columnNames
   */
  public String getColumnNames() {
    return columns;
  }

  /**
   * The method that puts the result coming from HBase into a Cascading tuple.
   * @param key the key for the rowresult
   * @param value the value map for the rowresult
   * @return the result tuple including the current RowResult
   */
  public Tuple source(Object key, Object value) {
    Tuple result = new Tuple();
    RowResult rowRes = (RowResult) value;

    result.add(rowRes);

    return result;
  }

  /**
   * The method the outputs data into HBase. The input to this method is
   * expected to be 1 tuple in the tupleEntry and in that tuple there should
   * only be one BatchUpdate.
   * @param tupleEntry the incoming data
   * @param outputCollector the collector to put the Batchupdate into
   */
  public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
  throws IOException {
    Tuple values = tupleEntry.getTuple();

    if(values.size() != 1){
      throw new IOException("The number of values in the tuple " +
          values.size() + " is not the expected 1");
    }
    //TODO should add check that the entry is a BatchUpdate
    outputCollector.collect(null, values.get(0));
  }


  public void sinkInit(Tap tap, JobConf conf) throws IOException {
    System.out.println("sinkinit: ");
    conf.setOutputFormat(TableOutputFormat.class);

    conf.setOutputKeyClass(ImmutableBytesWritable.class);
    conf.setOutputValueClass(BatchUpdate.class);
  }

  public void sourceInit(Tap tap, JobConf conf) throws IOException {
    conf.setInputFormat(TableInputFormat.class);

    LOG.debug("sourcing from columns: {}", columns);

    conf.set(TableInputFormat.COLUMN_LIST, columns);
  }

}
