/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cacading.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.hbase.HBaseScheme;
import cascading.hbase.HBaseTap;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.hbase.HBaseClusterTestCase;

/**
 *
 */
public class HBaseTest extends HBaseClusterTestCase
  {
  transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

  String inputFileLhs = "src/test/data/small.txt";

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

  public void testHbaseSingleFamily() throws IOException
    {
    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFileLhs );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), " " ) );

    Fields keyFields = new Fields( "num" );
    String familyName = "common";
    Fields valueFields = new Fields( "lower", "upper" );
    Tap hBaseTap = new HBaseTap( "commontable", new HBaseScheme( keyFields, familyName, valueFields ), SinkMode.REPLACE );

    Flow parseFlow = new FlowConnector( properties ).connect( source, hBaseTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 5 );

    // creaet flow to read from hbase and save to local file
    Tap sink = new Lfs( new TextLine(), "build/test/singlefamily/", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = new FlowConnector( properties ).connect( hBaseTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 5 );
    }

  public void testHbaseMultiFamily() throws IOException
    {
    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFileLhs );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), " " ) );

    Fields keyFields = new Fields( "num" );
    String[] familyNames = {"left", "right"};
    Fields[] valueFields = new Fields[] {new Fields( "lower" ), new Fields( "upper" ) };
    Tap hBaseTap = new HBaseTap( "commontable", new HBaseScheme( keyFields, familyNames, valueFields ), SinkMode.REPLACE );

    Flow parseFlow = new FlowConnector( properties ).connect( source, hBaseTap, parsePipe );

    parseFlow.complete();

    verifySink( parseFlow, 5 );

    // creaet flow to read from hbase and save to local file
    Tap sink = new Lfs( new TextLine(), "build/test/multifamily", SinkMode.REPLACE );

    Pipe copyPipe = new Each( "read", new Identity() );

    Flow copyFlow = new FlowConnector( properties ).connect( hBaseTap, sink, copyPipe );

    copyFlow.complete();

    verifySink( copyFlow, 5 );
    }

  private void verifySink( Flow flow, int expects ) throws IOException
    {
    int count = 0;

    TupleEntryIterator iterator = flow.openSink();
    while(iterator.hasNext())
      {
      count++;
      System.out.println( "iterator.next() = " + iterator.next() );
      }

    iterator.close();

    assertEquals( "wrong number of values", expects, count );
    }
  }
