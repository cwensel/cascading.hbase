/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
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
 * The HBaseTap class is a {@link Tap} subclass. It is used in conjunction with the {@HBaseScheme} to allow for the
 * reading and writing of data to and from a HBase cluster.
 *
 */
public class HBaseTap extends Tap
  {
  /** Field LOG  */
  private static final Logger LOG = LoggerFactory.getLogger( HBaseTap.class );

  /** Field SCHEME  */
  public static final String SCHEME = "hbase";

  /** Field tableName  */
  private String tableName;

  /** Field hBaseAdmin  */
  private transient HBaseAdmin hBaseAdmin;

  /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   *
   * @param tableName of type String
   * @param hBaseScheme of type HBaseScheme
   */
  public HBaseTap( String tableName, HBaseScheme hBaseScheme )
    {
    super( hBaseScheme, SinkMode.APPEND );

    this.tableName = tableName;
    }

  /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   *
   * @param tableName of type String
   * @param hBaseScheme of type HBaseScheme
   * @param sinkMode of type SinkMode
   */
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

    LOG.debug( "creating hbase table: {}", tableName );

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

    LOG.debug( "deleting hbase table: {}", tableName );

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
    LOG.debug( "sinking to table: {}", tableName );

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
  }
