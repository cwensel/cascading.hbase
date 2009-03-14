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
import java.net.URI;
import java.net.URISyntaxException;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class HBaseTapBase extends Tap
  {
  /** Field LOG */
  protected static final Logger LOG = LoggerFactory.getLogger( HBaseTapBase.class );
  /** Field SCHEME */
  public static final String SCHEME = "hbase";

  /** Field tableName */
  protected String tableName;

  protected HBaseTapBase( String tableName, Scheme scheme, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    this.tableName = tableName;
    }

  private URI getURI()
    {
    try
      {
      return new URI( SCHEME, tableName, null );
      }
    catch( URISyntaxException exception )
      {
      throw new TapException( "unable to create uri", exception );
      }
    }

  public Path getPath()
    {
    return new Path( getURI().toString() );
    }

  public boolean deletePath( JobConf conf ) throws IOException
    {
    // eventually keep table meta-data to source table create
    HBaseAdmin hBaseAdmin = new HBaseAdmin( new HBaseConfiguration( conf ) );

    if( !hBaseAdmin.tableExists( tableName ) )
      return true;

    LOG.debug( "deleting hbase table: {}", tableName );

    hBaseAdmin.disableTable( tableName );

    int count = 0;

    while( true )
      {
      if( !hBaseAdmin.isTableEnabled( tableName ) )
        break;

      if( count == 10 )
        throw new TapException( "unable to disable table: " + tableName );

      count++;

      try
        {
        Thread.sleep( 3 * 1000 );
        }
      catch( InterruptedException exception )
        {
        // ignore
        }
      }

    hBaseAdmin.deleteTable( tableName );

    return true;
    }

  public boolean pathExists( JobConf conf ) throws IOException
    {
    HBaseAdmin hBaseAdmin = new HBaseAdmin( new HBaseConfiguration( conf ) );
    return hBaseAdmin.tableExists( tableName );
    }

  public long getPathModified( JobConf conf ) throws IOException
    {
    return System.currentTimeMillis(); // currently unable to find last mod time on a table
    }

  public TupleEntryIterator openForRead( JobConf conf ) throws IOException
    {
    return new TupleEntryIterator( getSourceFields(), new TapIterator( this, conf ) );
    }

  public TupleEntryCollector openForWrite( JobConf conf ) throws IOException
    {
    return new TapCollector( this, conf );
    }

  @Override
  public void sinkInit( JobConf conf ) throws IOException
    {
    LOG.debug( "sinking to table: {}", tableName );

    // do not delete if initialized from within a task
    if( isReplace() && conf.get( "mapred.task.partition" ) == null )
      deletePath( conf );

    makeDirs( conf );

    conf.set( TableOutputFormat.OUTPUT_TABLE, tableName );
    super.sinkInit( conf );
    }

  @Override
  public void sourceInit( JobConf conf ) throws IOException
    {
    LOG.debug( "sourcing from table: {}", tableName );

    FileInputFormat.addInputPaths( conf, tableName );
    super.sourceInit( conf );
    }

  public boolean makeDirs( JobConf conf ) throws IOException
    {
    HBaseAdmin hBaseAdmin = new HBaseAdmin( new HBaseConfiguration( conf ) );

    if( hBaseAdmin.tableExists( tableName ) )
      return true;

    LOG.debug( "creating hbase table: {}", tableName );

    HTableDescriptor tableDescriptor = new HTableDescriptor( tableName );

    String[] familyNames = ( (HBaseSchemeBase) getScheme() ).getFamilyNames();

    for( String familyName : familyNames )
      tableDescriptor.addFamily( new HColumnDescriptor( familyName ) );

    hBaseAdmin.createTable( tableDescriptor );

    return true;
    }
  }
