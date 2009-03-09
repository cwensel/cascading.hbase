package cascading.hbase;

import java.util.Arrays;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;
import cascading.util.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.io.BatchUpdate;
//import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.io.RowResult;

/**
 * 
 */

/**
 * @author erik
 *
 */
public class DeFlatter extends BaseOperation implements Function {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger(DeFlatter.class);
  
  /** Field groupFieldSelector */
//  private Fields groupFieldSelector;
  /** Field resultFieldSelectors */
//  private Fields[] resultFieldSelectors;
//  private String family = "";
  
  
  /**
   * Constructor that takes no arguments
   *
   */  
  public DeFlatter() {
    super(Fields.ARGS);
  }

  
  /**
   * Constructor
   * @param fieldDeclaration, the fields to process
   *
   */  
  public DeFlatter(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }
 

  /* (non-Javadoc)
   * @see cascading.operation.Function#operate(cascading.flow.FlowProcess, cascading.operation.FunctionCall)
   */
  @Override
  public void operate(FlowProcess flow, FunctionCall fun) {
    TupleEntry input = fun.getArguments();
    TupleEntryCollector outputCollector = fun.getOutputCollector();

    Tuple tup = new Tuple(input.getTuple());
    Iterator<ImmutableBytesWritable> iter = input.getTuple().iterator();
    
    //Getting rowName
    BatchUpdate bu =  null;
    if(iter.hasNext()){
      bu = new BatchUpdate(iter.next().get());
    }

    //Putting keys and value in BatchUpdate
    byte[] key = null;
    byte[] val = null;
    while(iter.hasNext()){
      key = iter.next().get();
      if(iter.hasNext()){
        val = iter.next().get();
      } else {
        val = null;
        if(LOG.isInfoEnabled()){
          LOG.info(
            "Tuple to BatchUpdate ended with a key, setting value to null");
        }
      }
//      System.out.println("key "+new String(key)+ ", val " +new String(val) );
      bu.put(key, val);
    }
    outputCollector.add(new Tuple(bu));
  }

}
