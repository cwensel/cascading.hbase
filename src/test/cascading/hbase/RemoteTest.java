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
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Dfs;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class RemoteTest extends HBaseTestCase
{
    transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

    public RemoteTest()
    {
        super(1, false);
    }

    @Override
    protected void setUp() throws Exception
    {
        properties.put("fs.default.name", "hdfs://localhost:9000");
        properties.put("mapred.job.tracker", "localhost:9001");
        properties.put("hbase.master", "localhost:60000");
        properties.put("hbase.rootdir", "hdfs://localhost:9000/hbase");
    }
/*
    public void testCountColumn() throws IOException
    {
        Fields keyFields = new Fields("url");
        Fields valueFields = new Fields("base", "title");
        Tap hBaseTap = new HBaseTap("rsslists", new HBaseScheme(keyFields, valueFields), SinkMode.KEEP);

        Pipe assembly = new Pipe("wordcount");
        assembly = new GroupBy(assembly, new Fields("base"));
        Aggregator count = new Count(new Fields("count"));
        assembly = new Every(assembly, count);
        Tap sink = new Dfs(new TextLine(), "basecount", SinkMode.REPLACE);

        Flow countFlow = new FlowConnector(properties).connect(hBaseTap, sink, assembly);

        countFlow.complete();

    }
*/
    public void testColumnFamilyWithMultipleColumns() throws IOException
    {
        Fields keyFields = new Fields("url");
        Fields valueFields = new Fields("metric");
        Tap hBaseTap = new HBaseTap("urlmetrics", new HBaseScheme(keyFields, valueFields), SinkMode.KEEP);

        Tap sink = new Dfs(new TextLine(), "talents_export", SinkMode.REPLACE);

        //Pipe parsePipe = new Each( "read", new Identity());
        Pipe parsePipe = new Each( "insert", new Fields( "metric" ), new RegexSplitGenerator( new Fields( "col_val"), "," ) );
        parsePipe = new Each( parsePipe, new Fields( "col_val" ), new RegexSplitter( new Fields( "col", "val"), "=" ) );
        parsePipe = new GroupBy(parsePipe, new Fields("col"));
        parsePipe = new Every(parsePipe, new Count(new Fields("val")));

        Flow copyFlow = new FlowConnector(properties).connect(hBaseTap, sink, parsePipe);

        copyFlow.complete();

    }
}