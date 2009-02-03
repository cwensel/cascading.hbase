/*
 * This work is licensed under a Creative Commons Attribution-Share Alike 3.0 United States License.
 * http://creativecommons.org/licenses/by-sa/3.0/us/
 */

package cascading.hbase;

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

  public void testHBaseMultiFamily() throws IOException
    {
    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFileLhs );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), " " ) );

    Fields keyFields = new Fields( "num" );
    String[] familyNames = {"left", "right"};
    Fields[] valueFields = new Fields[] {new Fields( "lower" ), new Fields( "upper" ) };
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
