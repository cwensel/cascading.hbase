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
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;

/**
 *
 */
public class HBaseTap extends Tap
  {
  public static final String SCHEME = "hdfs";

  private String tableName;

  public HBaseTap( String tableName, HBaseScheme hBaseScheme )
    {
    super( hBaseScheme );

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
    return null;
    }

  public TupleEntryCollector openForWrite( JobConf conf ) throws IOException
    {
    return null;
    }

  public boolean makeDirs( JobConf conf ) throws IOException
    {
    // todo: create resource
    return true;
    }

  public boolean deletePath( JobConf conf ) throws IOException
    {
    // eventually keep table meta-data to source table create
    return true;
    }

  public boolean pathExists( JobConf conf ) throws IOException
    {
    return false;
    }

  public long getPathModified( JobConf conf ) throws IOException
    {
    return 0;
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
