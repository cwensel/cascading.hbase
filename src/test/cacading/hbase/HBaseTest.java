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
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 */
public class HBaseTest extends HBaseClusterTestCase
  {
  transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

  String inputFileLhs = "src/test/data/lhs.txt";
  String inputFileRhs = "src/test/data/rhs.txt";

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

  public void testHbase() throws IOException
    {
    String tableName = "testtable";
    String familyName = "testfamily";

    Tap source = new Lfs( new TextLine(), inputFileLhs );

    Pipe pipe = new Each( "lhs", new Fields( "line" ), new RegexSplitter( new Fields( "num", "char" ), " " ) );

    Tap sink = new HBaseTap( tableName, new HBaseScheme( familyName, new Fields( "num" ), new Fields( "char" ) ) );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    verify( tableName, "testfamily:char", 5 );
    }

  private void verify( String tableName, String charCol, int expected ) throws IOException
    {
    byte[][] columns = Bytes.toByteArrays( new String[]{charCol} );

    HTable table = new HTable( conf, tableName );
    Scanner scanner = table.getScanner( columns );

    System.out.println( "iterating scanner" );

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
