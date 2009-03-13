package cascading.hbase;

import java.util.Arrays;

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

/**
 * 
 */

/**
 * @author erik
 *
 */
public class AddFamily extends BaseOperation implements Function {
  
  /** Field groupFieldSelector */
  private Fields groupFieldSelector;
  /** Field resultFieldSelectors */
  private Fields[] resultFieldSelectors;
  private String family = "";
  
//  trace = Util.captureDebugTrace(this.getClass());

  
//  private int size = 1;
  /**
   * Constructor that takes no arguments
   *
   */  
  public AddFamily() {
    super(Fields.ARGS);
  }

  /**
   * Constructor that takes no arguments
   *
   */  
  public AddFamily(String family) {
    super(Fields.ARGS);
    this.family = family;
  }  
  
  /**
   * Constructor that takes no arguments
   *
   */  
  public AddFamily(Fields fieldDeclaration) {
    super(fieldDeclaration);
//    this.groupFieldSelector = null;
  }
 
  
  /**
   * Constructor UnGroup creates a new UnGroup instance.
   *
   * @param groupSelector  of type Fields
   * @param valueSelectors of type Fields[]
   */
  public AddFamily(Fields groupSelector, Fields[] valueSelectors) {
    int size = 0;

    for( Fields resultFieldSelector : valueSelectors ) {
      size = resultFieldSelector.size();
      numArgs = groupSelector.size() + size;

      if( fieldDeclaration.size() != numArgs )
        throw new IllegalArgumentException( "all field selectors must be the same size, and this size plus group selector size must equal the declared field size" );
    }

    this.groupFieldSelector = groupSelector;
    this.resultFieldSelectors = Arrays.copyOf( valueSelectors, valueSelectors.length );
    this.fieldDeclaration = Fields.size( groupSelector.size() + size );
  }

  /* (non-Javadoc)
   * @see cascading.operation.Function#operate(cascading.flow.FlowProcess, cascading.operation.FunctionCall)
   */
  @Override
  public void operate(FlowProcess flow, FunctionCall fun) {
    TupleEntry input = fun.getArguments();
    TupleEntryCollector outputCollector = fun.getOutputCollector();
    
//    System.out.println("input from operate " +input);
    
    // Getting columnName
    String col = (String)input.getTuple().get(1);
    String newCol = family +col;

//    if( types == null || types.length == 0 )
//      outputCollector.add( input );
//    else
    Tuple tup = new Tuple(input.getTuple());
//    System.out.println(tup);
    Comparable com = tup.get(1);
//    com = ((Long)com)+1;
//    tup.set(1, com);
    Tuple tu = new Tuple(tup.get(0), com);
    tup.set(1, newCol);
//    tu.add(com);
//    System.out.println(com);
//    tup.add(new Integer(1));
//    System.out.println("333333333333");
//    outputCollector.add(tu);
    outputCollector.add(tup);

//    outputCollector.add( Tuples.coerce( input.getTuple(), Integer.class ) );
  }

}
