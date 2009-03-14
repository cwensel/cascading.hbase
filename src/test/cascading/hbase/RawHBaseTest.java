package cascading.hbase;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Debug;
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
public class RawHBaseTest extends HBaseTestCase
  {
  transient static Map<Object, Object> properties = new HashMap<Object, Object>();

  static String inputFile = "src/test/data/small.txt";
  static String outputFile = "build/test/output/rawscheme";

  public RawHBaseTest()
    {
    super( 1, false );
    }

/**
 * Writes to HBase
 */
//  protected void hbaseTestWrite()
//  throws IOException {
//    inputFile = "/home/hbase/java/data/small.txt";
//
//    // create flow to read from local file and insert into HBase
//    Tap source = new Lfs( new TextLine(), inputFile );
//
//    Pipe pipe = new Each("insert", new Fields("line"),
//        new RegexSplitter(new Fields("row", "col", "data"), " "));
//
//    //Adding family to the cols
//    String family = "family:";
//    pipe = new Each(pipe, Fields.ALL, new AddFamily(family));
//
//    // Building the output format
////    Pipe outPipe = new Each(pipe, new StringDeFlatter(new Fields("bu")) );
//    pipe = new Each(pipe, new StringDeFlatter(new Fields("bu")) );
//
//    Tap hBaseTap = new HBaseTap("multitable1",
//      new HBaseScheme(), SinkMode.APPEND);
//
//    Flow parseFlow =
//      new FlowConnector(properties).connect(source, hBaseTap, pipe);
//    parseFlow.complete();
//
//    verifySource(parseFlow, 5);
////    verifySink(parseFlow, 5);
//  }

  /** Modified tester taken from cascading.hbase */
  public void testRawScheme() throws Exception
    {
    Tap localSource = new Lfs( new TextLine(), inputFile );

    Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "ignore", "family:lower", "family:upper" ), " " ) );
    parsePipe = new Each( parsePipe, new ExpressionFunction( new Fields( "num" ), "(int) (Math.random() * Integer.MAX_VALUE)" ), Fields.ALL );
//    parsePipe = new Each( parsePipe, new Debug() );

    Tap hBaseTap = new HBaseTap( "rawtable", new HBaseScheme( new Fields( "num" ), new Fields( "family:lower" ) ), SinkMode.REPLACE );

    Flow loadFlow = new FlowConnector( properties ).connect( localSource, hBaseTap, parsePipe );

    loadFlow.complete();

    Tap source = new HBaseTap( "rawtable", new HBaseRawScheme( new Fields( "table" ), "family:" ) );

    Pipe pipe = new Pipe( "write" );

    //Flatten the RowResult from HBase into a tuple stream
    pipe = new Each( pipe, new Flatter() );

    //DeFlatten the tuple stream into a Batchupdate
    pipe = new Each( pipe, new DeFlatter( new Fields( "batchupdate" ) ) );
    pipe = new Each( pipe, new Debug() );

    Tap sink = new HBaseTap( "rawtable-target", new HBaseRawScheme( new Fields( "batchupdate" ) ), SinkMode.REPLACE );

    Flow flow = new FlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    verifySource( flow, 13 );
    verifySink( flow, 1 );
    }

  }
