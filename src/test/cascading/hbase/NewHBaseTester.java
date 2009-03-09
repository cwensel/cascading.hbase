package test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;

import cascading.hbase.HBaseScheme;
import cascading.hbase.HBaseTap;

import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.Debug;
import cascading.operation.filter.Limit;
import cascading.operation.function.UnGroup;

import cascading.operation.regex.RegexSplitter;
import cascading.operation.regex.*;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

// CascadingTesters
//import cascadingTester.operation.functions.Iden;
import cascading.hbase.AddFamily;
import cascading.hbase.DeFlatter;
import cascading.hbase.Flatter;
import cascading.hbase.StringDeFlatter;

// HBase
import org.apache.hadoop.hbase.HBaseClusterTestCase;
 
/**
*
*/
public class NewHBaseTester extends HBaseClusterTestCase {
  
  public NewHBaseTester( int i, boolean b ) {
    super( i, b );
  }
  
  
  transient static Map<Object, Object> properties =
    new HashMap<Object,Object>();
  static String inputFile =  "/home/hbase/java/data/small.txt";
  static String outputFile = "/home/hbase/java/data/res/";  
  
//   private FileDeleter fd = new FileDeleter(outputFile); 

  public static void main(String[] args)
  throws IOException {
    // HBaseTester
//    hbaseTester();
    
//    hbaseTestWrite();
    hbaseTestRead();

    // Tester1
//    test1();
    
    //Import users to HBase
//    userImport();
  }


  /**
   * Writes to HBase
   */ 
  public static void hbaseTestWrite()
  throws IOException {
    inputFile = "/home/hbase/java/data/small.txt";

    // create flow to read from local file and insert into HBase
    Tap source = new Lfs( new TextLine(), inputFile );

    Pipe pipe = new Each("insert", new Fields("line"),
        new RegexSplitter(new Fields("row", "col", "data"), " "));
    
    //Adding family to the cols
    String family = "family:";
    pipe = new Each(pipe, Fields.ALL, new AddFamily(family));
    
    // Building the output format
//    Pipe outPipe = new Each(pipe, new StringDeFlatter(new Fields("bu")) );
    pipe = new Each(pipe, new StringDeFlatter(new Fields("bu")) );
    
    Tap hBaseTap = new HBaseTap("multitable1",
      new HBaseScheme(), SinkMode.APPEND);
    
    Flow parseFlow =
      new FlowConnector(properties).connect(source, hBaseTap, pipe);
    parseFlow.complete();

    verifySource(parseFlow, 5);
//    verifySink(parseFlow, 5);
  }  
  
  /**
   * Modified tester taken from cascading.hbase
   */   
  public static void hbaseTestRead()
  throws IOException {
    String output = "build/test/multifamily";
    FileDeleter fdt = new FileDeleter(outputFile);
    
    String columns = "family:";
    Tap source = new HBaseTap("multitable1",
      new HBaseScheme(columns), SinkMode.APPEND);
    
    Pipe pipe = null;
    //Flatten the RowResult from HBase into a tuple stream
    pipe = new Each(pipe, new Flatter() );
    
    //DeFlatten the tuple stream into a Batchupdate
    pipe = new Each(pipe, new DeFlatter(new Fields("bu")) );
    
    Tap sink = new HBaseTap("multitable1",
        new HBaseScheme(), SinkMode.APPEND);
    
//    Tap sink = new Lfs(
//      new TextLine(), output, SinkMode.APPEND);
    
    Flow flow =
      new FlowConnector(properties).connect(source, sink, pipe);

    flow.complete();
    
    verifySource(flow, 5);
    verifySink(flow, 5);  
  }
  

  
  // Helpers
  private static void verifySource(Flow flow, int expects)
  throws IOException {
    int count = 0;
  
    TupleEntryIterator iterator = flow.openSource();
    System.out.println("sourceIterator.hasNext " +iterator.hasNext());
    while(iterator.hasNext()) {
      count++;
      System.out.println("iterator.next() = " + iterator.next());
    }
  
    iterator.close();
  }
  
  private static void verifySink(Flow flow, int expects)
  throws IOException {
    
    int count = 0;
  
    TupleEntryIterator iterator = flow.openSink();
    System.out.println("sinkIterator.hasNext " +iterator.hasNext());
    while(iterator.hasNext()) {
      count++;
      System.out.println("iterator.next() = " + iterator.next());
    }
  
    iterator.close();
  }


  private static class FileDeleter{
    public FileDeleter(String file){
      boolean res = deleteDir(new File(file));
    }
    
    private boolean deleteDir(File dir) {
      if (dir.isDirectory()) {
        String[] children = dir.list();
        for (int i=0; i<children.length; i++) {
          boolean success = deleteDir(new File(dir, children[i]));
          if (!success) {
            return false;
          }
        }
      }
      
      // The directory is now empty so delete it
      return dir.delete();
    }
  }
  
}
