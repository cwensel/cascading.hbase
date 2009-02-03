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
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * todo: take a map of family name to valueFields, automatically scope incoming fields to the family
 */
public class HBaseScheme extends Scheme
  {
  private Fields keyFields;
  private String[] familyNames;
  private Fields[] valueFields;

  private transient byte[][] fields;

  public HBaseScheme( Fields keyFields, String familyName, Fields valueFields )
    {
    this( keyFields, new String[]{familyName}, Fields.fields( valueFields ) );
    }

  public HBaseScheme( Fields keyFields, String[] familyNames, Fields[] valueFields )
    {
    this.keyFields = keyFields;
    this.familyNames = familyNames;
    this.valueFields = valueFields;

    setSourceSink( this.keyFields, this.valueFields );

    validate();
    }

  private void validate()
    {
    if( keyFields.size() != 1 )
      throw new IllegalArgumentException( "may only have one key field, found: " + keyFields.print() );
    }

  private void setSourceSink( Fields keyFields, Fields[] valueFields )
    {
    Fields allFields = Fields.join( keyFields, Fields.join( valueFields ) ); // prepend keys

    setSourceFields( allFields );
    setSinkFields( allFields );
    }

  public String[] getFamilyNames()
    {
    return familyNames;
    }

  private byte[][] getFieldsBytes()
    {
    if( fields == null )
      fields = makeBytes( familyNames, valueFields );

    return fields;
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

    for( int i = 0; i < valueFields.length; i++ )
      {
      Fields fieldSelector = valueFields[ i ];
      TupleEntry values = tupleEntry.selectEntry( fieldSelector );

      for( int j = 0; j < values.getFields().size(); j++ )
        {
        // family_name:column_name
        Fields fields = values.getFields();
        Tuple tuple = values.getTuple();
        batchUpdate.put( familyNames[ i ] + ":" + fields.get( j ), Bytes.toBytes( tuple.getString( j ) ) );
        }
      }

    outputCollector.collect( null, batchUpdate );
    }

  public void sinkInit( Tap tap, JobConf conf ) throws IOException
    {
    conf.setOutputFormat( TableOutputFormat.class );

    conf.setOutputKeyClass( ImmutableBytesWritable.class );
    conf.setOutputValueClass( BatchUpdate.class );
    }

  public void sourceInit( Tap tap, JobConf conf ) throws IOException
    {
    conf.setInputFormat( TableInputFormat.class );

    conf.set( TableInputFormat.COLUMN_LIST, getColumns() );
    }

  private String getColumns()
    {
    return Util.join( columns( familyNames, valueFields ), " " );
    }

  private String[] columns( String[] familyNames, Fields[] fieldsArray )
    {
    int size = 0;

    for( Fields fields : fieldsArray )
      size += fields.size();

    String[] columns = new String[size];

    for( int i = 0; i < fieldsArray.length; i++ )
      {
      Fields fields = fieldsArray[ i ];

      for( int j = 0; j < fields.size(); j++ )
        columns[ i + j ] = familyNames[ i ] + ":" + (String) fields.get( j );
      }

    return columns;
    }

  private byte[][] makeBytes( String[] familyNames, Fields[] fieldsArray )
    {
    String[] columns = columns( familyNames, fieldsArray );
    byte[][] bytes = new byte[columns.length][];

    for( int i = 0; i < columns.length; i++ )
      bytes[ i ] = Bytes.toBytes( columns[ i ] );

    return bytes;
    }

  }
