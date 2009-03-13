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

/**
 *
 */
public class MultiFamilyCascadeHBaseTest extends HBaseTestCase
  {
  transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

  String inputFile = "src/test/data/small.txt";

  public MultiFamilyCascadeHBaseTest()
    {
    super( 1, false );
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();

//    MultiMapReducePlanner.setJobConf( properties, conf );
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

  }
