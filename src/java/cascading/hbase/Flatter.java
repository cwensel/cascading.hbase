package cascading.hbase;

import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */

/** @author erik */
public class Flatter extends BaseOperation implements Function
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( Flatter.class );

  /** Field groupFieldSelector */
//  private Fields groupFieldSelector;
  /** Field resultFieldSelectors */
//  private Fields[] resultFieldSelectors;
//  private String family = "";


  /** Constructor that takes no arguments */
  public Flatter()
    {
    super( Fields.ARGS );
    }


  /**
   * Constructor
   *
   * @param fieldDeclaration, the fields to process
   */
  public Flatter( Fields fieldDeclaration )
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

    Tuple tup = new Tuple();
    //Assuming that the only thing in the tuple is the RowResult
    RowResult rr = (RowResult) input.getTuple().get( 0 );

    //Adding row name
    tup.add( new ImmutableBytesWritable( rr.getRow() ) );

    //Adding column and value
    ImmutableBytesWritable key = null;
    ImmutableBytesWritable val = null;
    for( Map.Entry<byte[], Cell> entry : rr.entrySet() )
      {
      key = new ImmutableBytesWritable( entry.getKey() );
      val = new ImmutableBytesWritable( entry.getValue().getValue() );
      tup.add( key );
      tup.add( val );
      }

    outputCollector.add( tup );
    }

  }
