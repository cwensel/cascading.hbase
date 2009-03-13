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
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Dfs;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class ColumnFamilyTest extends HBaseTestCase
{
    transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

    public ColumnFamilyTest()
    {
        super(1, false);
    }

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
    }

    public void testColumnFamilyWithMultipleColumns() throws IOException
    {
        Fields keyFields = new Fields("key");
        Fields valueFields = new Fields("family");
        Tap hBaseTap = new HBaseTap("test_table", new HBaseScheme(keyFields, valueFields), SinkMode.KEEP);

        Tap sink = new Dfs(new TextLine(), "results", SinkMode.REPLACE);

        //Pipe parsePipe = new Each( "read", new Identity());
        Pipe parsePipe = new Each( "insert", new Fields( "family" ), new RegexSplitGenerator( new Fields( "col_val"), "," ) );
        parsePipe = new Each( parsePipe, new Fields( "col_val" ), new RegexSplitter( new Fields( "col", "val"), "=" ) );
        parsePipe = new GroupBy(parsePipe, new Fields("col"));
        parsePipe = new Every(parsePipe, new Count(new Fields("val")));

        Flow copyFlow = new FlowConnector(properties).connect(hBaseTap, sink, parsePipe);

        copyFlow.complete();

    }


}