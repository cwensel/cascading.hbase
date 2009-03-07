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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
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
public class MultiFamilyFullyQualifiedTest extends HBaseTestCase
  {
  transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

  String inputFile = "src/test/data/small.txt";

  public MultiFamilyFullyQualifiedTest()
    {
    super( 1, false );
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();

    // MultiMapReducePlanner.setJobConf( properties, conf );
    }

  public void testHBaseMultiFamily() throws IOException
    {
    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFile );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "num", "left:lower", "right" ), " " ) );

    Fields keyFields = new Fields( "num" );
    Fields valueFields = new Fields( "left:lower", "right" );
    Tap hBaseTap = new HBaseTap( "multitable", new HBaseScheme( keyFields, valueFields ), SinkMode.REPLACE );

    Flow parseFlow = new FlowConnector( properties ).connect( source, hBaseTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 5 );

//        Pipe assembly = new Pipe("wordcount");
//        assembly = new GroupBy(assembly, new Fields("left:lower"));
//        Aggregator count = new Count(new Fields("count"));
//        assembly = new Every(assembly, count);
//
//        Tap sink = new Lfs(new TextLine(), "build/test/wordcount", SinkMode.REPLACE);
//
//        Flow countFlow = new FlowConnector(properties).connect(hBaseTap, sink, assembly);
//
//        countFlow.complete();

//        verifySink(countFlow, 3);

    }
  }