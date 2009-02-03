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

import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.TapIterator;
import cascading.tap.hadoop.TapCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.flow.Flow;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HBaseTap extends Tap
  {
  private static final Logger LOG = LoggerFactory.getLogger( HBaseTap.class );

  public static final String SCHEME = "hdfs";

  private String tableName;

  private transient HBaseAdmin hBaseAdmin;

  public HBaseTap( String tableName, HBaseScheme hBaseScheme )
    {
    super( hBaseScheme, SinkMode.APPEND );

    this.tableName = tableName;
    }

  public HBaseTap( String tableName, HBaseScheme hBaseScheme, SinkMode sinkMode )
    {
    super( hBaseScheme, sinkMode );

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

  public TupleEntryIterator openForRead( JobConf conf ) throws IOException
    {
    return new TupleEntryIterator( getSourceFields(), new TapIterator( this, conf ) );
    }

  public TupleEntryCollector openForWrite( JobConf conf ) throws IOException
    {
    return new TapCollector( this, conf );
    }

  private HBaseAdmin getHBaseAdmin() throws MasterNotRunningException
    {
    if( hBaseAdmin == null )
      hBaseAdmin = new HBaseAdmin( new HBaseConfiguration() );

    return hBaseAdmin;
    }

  public boolean makeDirs( JobConf conf ) throws IOException
    {
    HBaseAdmin hBaseAdmin = getHBaseAdmin();

    if( hBaseAdmin.tableExists( tableName ) )
      return true;

    LOG.debug("creating hbase table: {}", tableName );

    HTableDescriptor tableDescriptor = new HTableDescriptor( tableName );

    String[] familyNames = ( (HBaseScheme) getScheme() ).getFamilyNames();

    for( String familyName : familyNames )
      tableDescriptor.addFamily( new HColumnDescriptor( familyName + ":" ) );

    hBaseAdmin.createTable( tableDescriptor );

    return true;
    }

  public boolean deletePath( JobConf conf ) throws IOException
    {
    // eventually keep table meta-data to source table create
    HBaseAdmin hBaseAdmin = getHBaseAdmin();

    if( !hBaseAdmin.tableExists( tableName ) )
      return true;

    LOG.debug("deleting hbase table: {}", tableName );

    hBaseAdmin.disableTable( tableName );
    hBaseAdmin.deleteTable( tableName );

    return true;
    }

  public boolean pathExists( JobConf conf ) throws IOException
    {
    return getHBaseAdmin().tableExists( tableName );
    }

  public long getPathModified( JobConf conf ) throws IOException
    {
    return System.currentTimeMillis(); // currently unable to find last mod time on a table
    }

  @Override
  public void flowInit( Flow flow )
    {
    try
      {
      makeDirs( flow.getJobConf() );
      }
    catch( IOException exception )
      {

      }
    super.flowInit( flow );
    }

  @Override
  public void sinkInit( JobConf conf ) throws IOException
    {
    conf.set( TableOutputFormat.OUTPUT_TABLE, tableName );
    super.sinkInit( conf );
    }

  @Override
  public void sourceInit( JobConf conf ) throws IOException
    {
    FileInputFormat.addInputPaths( conf, tableName );
    super.sourceInit( conf );
    }
  }
