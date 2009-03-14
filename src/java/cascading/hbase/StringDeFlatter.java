package cascading.hbase;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.hadoop.hbase.io.RowResult;

/**
 *
 */

/** @author erik */
public class StringDeFlatter extends BaseOperation implements Function
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( StringDeFlatter.class );

  /** Field groupFieldSelector */
//  private Fields groupFieldSelector;
  /** Field resultFieldSelectors */
//  private Fields[] resultFieldSelectors;
//  private String family = "";


  /**
   * Constructor that takes no arguments
   *
   */
//  public StringDeFlatter() {
//    super(Fields.ARGS);
//  }


  /**
   * Constructor
   *
   * @param fieldDeclaration, the fields to output
   */
  public StringDeFlatter( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    }


  /* (non-Javadoc)
  * @see cascading.operation.Function#operate(cascading.flow.FlowProcess, cascading.operation.FunctionCall)
  */
  public void operate( FlowProcess flow, FunctionCall fun )
    {
    TupleEntry input = fun.getArguments();
    TupleEntryCollector outputCollector = fun.getOutputCollector();

    Tuple tup = new Tuple( input.getTuple() );
    Iterator<String> iter = input.getTuple().iterator();

    //Getting rowName
    BatchUpdate bu = null;
    if( iter.hasNext() )
      {
//      da = iter.next();
//      System.out.println("row da " +da);
//      bu = new BatchUpdate(da.getBytes());
      bu = new BatchUpdate( iter.next().getBytes() );
      }

    //Putting keys and value in BatchUpdate
    byte[] key = null;
    byte[] val = null;
    while( iter.hasNext() )
      {
      key = iter.next().getBytes();
      if( iter.hasNext() )
        {
        val = iter.next().getBytes();
        }
      else
        {
        val = null;
        if( LOG.isInfoEnabled() )
          {
          LOG.info( "Tuple to BatchUpdate ended with a key, setting value to null" );
          }
        }
      bu.put( key, val );
      }

    outputCollector.add( new Tuple( bu ) );
    }

  }
