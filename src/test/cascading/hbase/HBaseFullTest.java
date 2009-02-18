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
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class HBaseFullTest extends TestCase {
	transient private static Properties properties = new Properties();

	String inputFile = "src/test/data/small.txt";

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		properties.setProperty("fs.default.name", "hdfs://localhost:9000");
		properties.setProperty("mapred.job.tracker", "localhost:9001");
		properties.setProperty("hbase.master", "localhost:60000");
		properties.setProperty("hbase.rootdir", "hdfs://localhost:9000/hbase");

	}

	@Test
	public void testHBaseMultiFamily() throws IOException {
		// create flow to read from local file and insert into HBase
		Tap source = new Lfs(new TextLine(), inputFile);

		Pipe parsePipe = new Each("insert", new Fields("line"),
				new RegexSplitter(new Fields("num", "content:lower", "content:upper"), " "));

		String keyName = "num";
		String[] columnNames = { "content:lower", "content:upper" };
		Tap hBaseTap = new HBaseFullTap("multitable", new HBaseFullScheme(
				keyName, columnNames), SinkMode.REPLACE);

		Flow parseFlow = new FlowConnector(properties).connect(source,
				hBaseTap, parsePipe);

		parseFlow.complete();

		verifySink(parseFlow, 5);

		// create flow to read from hbase and save to local file
		Tap sink = new Hfs(new TextLine(), "multifamily",
				SinkMode.REPLACE);

		Pipe copyPipe = new Each("read", new Identity());

		Flow copyFlow = new FlowConnector(properties).connect(hBaseTap, sink,
				copyPipe);

		copyFlow.complete();

		verifySink(copyFlow, 5);
	}

	private void verifySink(Flow flow, int expects) throws IOException {
		int count = 0;

		TupleEntryIterator iterator = flow.openSink();

		while (iterator.hasNext()) {
			count++;
			System.out.println("iterator.next() = " + iterator.next());
		}

		iterator.close();

		assertEquals("wrong number of values", expects, count);
	}

	@Test
	public void testGroupByCount() throws IOException {

		String keyName = "num";
		String[] columnNames = { "content:lower", "content:upper" };
		Tap source = new HBaseFullTap("multitable", new HBaseFullScheme(keyName,
				columnNames), SinkMode.REPLACE);

		Scheme sinkScheme = new TextLine(new Fields("content:lower", "count"));
		Tap sink = new Hfs(sinkScheme, "lowercount", true);

		Pipe assembly = new Pipe("lowercount");
		assembly = new GroupBy(assembly, new Fields("content:lower"));
		Aggregator count = new Count(new Fields("count"));
		assembly = new Every(assembly, count);

		Flow copyFlow = new FlowConnector(properties).connect(source, sink,
				assembly);

		copyFlow.complete();

	}

}
