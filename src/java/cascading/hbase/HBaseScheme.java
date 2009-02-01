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

package cascading.hbase;

import java.io.IOException;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tap.Tap;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

/**
 *
 */
public class HBaseScheme extends Scheme
  {
  private String familyName;
  private Fields keyFields;
  private Fields valueFields;
  private byte[][] fields;


  public HBaseScheme( String familyName, Fields keyFields, Fields valueFields )
    {
    this.familyName = familyName;
    this.keyFields = keyFields;
    this.valueFields = valueFields;

    setSourceSink( keyFields, valueFields );

    validate();
    }

  public HBaseScheme( String familyName, Fields keyFields, Fields valueFields, int numSinkParts )
    {
    this.familyName = familyName;
    this.keyFields = keyFields;
    this.valueFields = valueFields;

    setSourceSink( keyFields, valueFields );
    setNumSinkParts( numSinkParts );

    validate();
    }

  private void validate()
    {
    if( keyFields.size() != 1 )
      throw new IllegalArgumentException( "may only have one key field, found: " + keyFields.print() );
    }

  private void setSourceSink( Fields keyFields, Fields valueFields )
    {
    Fields allFields = Fields.join( keyFields, valueFields );

    setSourceFields( allFields );
    setSinkFields( allFields );
    }

  private byte[][] getFieldsBytes()
    {
    if( fields == null )
      fields = makeBytes( valueFields );

    return fields;
    }

  private byte[][] makeBytes( Fields fields )
    {
    byte[][] bytes = new byte[fields.size()][];

    for( int i = 0; i < fields.size(); i++ )
      bytes[ i ] = Bytes.toBytes( familyName + ":" + (String) fields.get( i ) );

    return bytes;
    }

  public Tuple source( Object key, Object value )
    {
    Tuple result = new Tuple();

    ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
    RowResult row = (RowResult) value;

    result.add( Bytes.toString( keyWritable.get() ) );

    for( byte[] bytes : getFieldsBytes() )
      {
      Cell cell = row.get( bytes );
      result.add( Bytes.toString( cell.getValue() ) );
      }

    return result;
    }

  public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
    {
    Tuple key = tupleEntry.selectTuple( keyFields );

    byte[] keyBytes = Bytes.toBytes( key.getString( 0 ) );
    BatchUpdate batchUpdate = new BatchUpdate( keyBytes );

    TupleEntry values = tupleEntry.selectEntry( valueFields );

    for( int i = 0; i < values.getFields().size(); i++ )
      {
      // family_name:column_name
      Fields fields = values.getFields();
      Tuple tuple = values.getTuple();
      batchUpdate.put( familyName + ":" + fields.get( i ), Bytes.toBytes( tuple.getString( i ) ) );
      }

    outputCollector.collect( null, batchUpdate );
    }

  public void sinkInit( Tap tap, JobConf conf ) throws IOException
    {
    conf.setOutputFormat( TableOutputFormat.class );

    conf.setOutputKeyClass( ImmutableBytesWritable.class );
    conf.setOutputValueClass( BatchUpdate.class );
//    conf.setPartitionerClass( HRegionPartitioner.class );

//    HTable outputTable = new HTable( new HBaseConfiguration( conf ), table );
//    int regions = outputTable.getRegionsInfo().size();
//
//    if( conf.getNumReduceTasks() > regions )
//      conf.setNumReduceTasks( outputTable.getRegionsInfo().size() );
    }

  public void sourceInit( Tap tap, JobConf conf ) throws IOException
    {
    conf.setInputFormat( TableInputFormat.class );

    conf.set( TableInputFormat.COLUMN_LIST, getColumns() );
    }

  private String getColumns()
    {
    String[] columns = new String[valueFields.size()];

    for( int i = 0; i < columns.length; i++ )
      columns[ i ] = valueFields.get( i ).toString();

    return Util.join( columns, " " );
    }

  }
