/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 */
public class HBaseTest extends HBaseClusterTestCase
  {
  transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

  String inputFile = "src/test/data/small.txt";

  public HBaseTest()
    {
    super( 1, false );
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();

//    MultiMapReducePlanner.setJobConf( properties, conf );
    }

  public void testHBaseMultiFamily() throws IOException
    {
    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFile );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), " " ) );

    Fields keyFields = new Fields( "num" );
    String[] familyNames = {"left", "right"};
    Fields[] valueFields = new Fields[]{new Fields( "lower" ), new Fields( "upper" )};
    Tap hBaseTap = new HBaseTap( "multitable", new HBaseScheme( keyFields, familyNames, valueFields ), SinkMode.REPLACE );

    Flow parseFlow = new FlowConnector( properties ).connect( source, hBaseTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 5 );

    // create flow to read from hbase and save to local file
    Tap sink = new Lfs( new TextLine(), "build/test/multifamily", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = new FlowConnector( properties ).connect( hBaseTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 5 );
    }

  public void testHBaseMultiFamilyCascade() throws IOException
    {
    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFile );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "ignore", "lower", "upper" ), " " ) );
    parsePipe = new Each( parsePipe, new ExpressionFunction( new Fields( "num" ), "(int) (Math.random() * Integer.MAX_VALUE)" ), Fields.ALL );
//    parsePipe = new Each( parsePipe, new Debug() );

    Fields keyFields = new Fields( "num" );
    String[] familyNames = {"left", "right"};
    Fields[] valueFields = new Fields[]{new Fields( "lower" ), new Fields( "upper" )};
    Tap hBaseTap = new HBaseTap( "multitable", new HBaseScheme( keyFields, familyNames, valueFields ) );

    Flow parseFlow = new FlowConnector( properties ).connect( source, hBaseTap, parsePipe );

    // create flow to read from hbase and save to local file
    Tap sink = new Lfs( new TextLine(), "build/test/multifamilycascade", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = new FlowConnector( properties ).connect( hBaseTap, sink, copyPipe );

    Cascade cascade = new CascadeConnector().connect( copyFlow, parseFlow ); // reversed order intentionally

    parseFlow.deleteSinks(); // force the hbase tap to be deleted

    cascade.complete();

    verify( "multitable", "left:lower", 13 );

    verifySink( parseFlow, 13 );
    verifySink( copyFlow, 13 );

    parseFlow = new FlowConnector( properties ).connect( source, hBaseTap, parsePipe );
    copyFlow = new FlowConnector( properties ).connect( hBaseTap, sink, copyPipe );

    cascade = new CascadeConnector().connect( copyFlow, parseFlow ); // reversed order intentionally

    cascade.complete();

    verify( "multitable", "left:lower", 26 );

    verifySink( parseFlow, 26 );
    verifySink( copyFlow, 26 );
    }

  private void verifySink( Flow flow, int expects ) throws IOException
    {
    int count = 0;

    TupleEntryIterator iterator = flow.openSink();

    while( iterator.hasNext() )
      {
      count++;
      System.out.println( "iterator.next() = " + iterator.next() );
      }

    iterator.close();

    assertEquals( "wrong number of values in " + flow.getSink().toString(), expects, count );
    }

  private void loadTable( String tableName, String charCol, int size ) throws IOException
    {
    HTable table = new HTable( conf, tableName );

    for( int i = 0; i < size; i++ )
      {
      byte[] bytes = Bytes.toBytes( Integer.toString( i ) );
      BatchUpdate batchUpdate = new BatchUpdate( bytes );

      batchUpdate.put( charCol, bytes );

      table.commit( batchUpdate );
      }

    table.close();
    }

  private void verify( String tableName, String charCol, int expected ) throws IOException
    {
    byte[][] columns = Bytes.toByteArrays( new String[]{charCol} );

    HTable table = new HTable( conf, tableName );
    Scanner scanner = table.getScanner( columns );

    int count = 0;
    for( RowResult rowResult : scanner )
      {
      count++;
      System.out.println( "rowResult = " + rowResult.get( charCol ) );
      }

    scanner.close();

    assertEquals( "wrong number of rows", expected, count );
    }
  }
