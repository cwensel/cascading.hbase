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

import cascading.tap.SinkMode;
import cascading.tap.Tap;

/**
 * The HBaseTap class is a {@link Tap} subclass. It is used in conjunction with the {@HBaseFullScheme}
 * to allow for the reading and writing of data to and from a HBase cluster.
 */
public class HBaseTap extends HBaseTapBase
  {

  /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   *
   * @param tableName   of type String
   * @param hBaseScheme of type HBaseFullScheme
   */
  public HBaseTap( String tableName, HBaseScheme hBaseScheme )
    {
    super( tableName, hBaseScheme, SinkMode.APPEND );
    }

  /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   *
   * @param tableName   of type String
   * @param hBaseScheme of type HBaseFullScheme
   * @param sinkMode    of type SinkMode
   */
  public HBaseTap( String tableName, HBaseScheme hBaseScheme, SinkMode sinkMode )
    {
    super( tableName, hBaseScheme, sinkMode );
    }
  }