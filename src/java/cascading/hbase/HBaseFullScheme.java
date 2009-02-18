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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tap.Tap;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HBaseScheme class is a {@link Scheme} subclass. It is used in conjunction
 * with the {@HBaseTap} to allow for the reading and writing of data
 * to and from a HBase cluster.
 * 
 * @see HBaseTap
 */
public class HBaseFullScheme extends Scheme {
	/** Field LOG */
	private static final Logger LOG = LoggerFactory
			.getLogger(HBaseFullScheme.class);

	/** Field keyFields */
	private String keyName;
	private Fields keyFields;

	/** Field valueFields */
	private String[] columnNames;
	private Fields[] columnFields;
	private byte[][] fields = null;

	/**
	 * Constructor HBaseScheme creates a new HBaseScheme instance.
	 * 
	 * @param keyName
	 *            of type String
	 * @param columnName
	 *            of type String
	 */
	public HBaseFullScheme(String keyName, String columnName) {
		this(keyName, new String[] { columnName });
	}

	/**
	 * Constructor HBaseScheme creates a new HBaseScheme instance.
	 * 
	 * @param keyName
	 *            of type String
	 * @param columnNames
	 *            of type String[]
	 */
	public HBaseFullScheme(String keyName, String[] columnNames) {
		this.keyName = keyName;
		this.keyFields = new Fields(keyName);
		this.columnNames = columnNames;

		this.columnFields = new Fields[columnNames.length];
		for (int i = 0; i < columnNames.length; i++) {
			this.columnFields[i] = new Fields(columnNames[i]);
		}

		setSourceSink(this.keyFields, this.columnFields);

	}

	private void setSourceSink(Fields keyFields, Fields[] columnFields) {
		Fields allFields = Fields.join(keyFields, Fields.join(columnFields)); // prepend
		// keys

		setSourceFields(allFields);
		setSinkFields(allFields);
	}

	public String getKeyName() {
		return keyName;
	}

	/**
	 * Method getFamilyNames returns the familyNames of this HBaseScheme object.
	 * 
	 * @return the familyNames (type String[]) of this HBaseScheme object.
	 */
	public String[] getFamilyNames() {
		HashSet<String> familyNames = new HashSet<String>();
		for (String columnName : columnNames) {
			int pos = columnName.indexOf(":");
			if (pos > 0) {
				familyNames.add(columnName.substring(0, pos + 1));
			}
			else {
				familyNames.add(columnName + ":");
			}
		}
		return familyNames.toArray(new String[0]);
	}

	public Tuple source(Object key, Object value) {
		Tuple result = new Tuple();

		ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
		RowResult row = (RowResult) value;

		result.add(Bytes.toString(keyWritable.get()));

		for (byte[] bytes : getFields(this.columnNames)) {
			Cell cell = row.get(bytes);
			result.add(cell!=null?Bytes.toString(cell.getValue()):"");
		}

		return result;
	}

	private byte[][] getFields(String[] columnNames) {
		if (fields == null)
			fields = new byte[columnNames.length][];

		for (int i = 0; i < columnNames.length; i++)
			fields[i] = Bytes.toBytes(hbaseColumn(columnNames[i]));

		return fields;
	}

	public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
			throws IOException {
		Tuple key = tupleEntry.selectTuple(keyFields);

		byte[] keyBytes = Bytes.toBytes(key.getString(0));
		BatchUpdate batchUpdate = new BatchUpdate(keyBytes);

		for (int i = 0; i < columnFields.length; i++) {
			Fields fieldSelector = columnFields[i];
			TupleEntry values = tupleEntry.selectEntry(fieldSelector);

			for (int j = 0; j < values.getFields().size(); j++) {
				Fields fields = values.getFields();
				Tuple tuple = values.getTuple();
				batchUpdate.put(hbaseColumn(fields.get(j).toString()), Bytes
						.toBytes(tuple.getString(j)));
			}
		}

		outputCollector.collect(null, batchUpdate);
	}

	public void sinkInit(Tap tap, JobConf conf) throws IOException {
		conf.setOutputFormat(TableOutputFormat.class);

		conf.setOutputKeyClass(ImmutableBytesWritable.class);
		conf.setOutputValueClass(BatchUpdate.class);
	}

	public void sourceInit(Tap tap, JobConf conf) throws IOException {
		conf.setInputFormat(TableInputFormat.class);

		String columns = getColumns();
		LOG.debug("sourcing from columns: {}", columns);

		conf.set(TableInputFormat.COLUMN_LIST, columns);
	}

	private String getColumns() {
		String columns = new String();
		for (String column : columnNames) {
			columns += hbaseColumn(column) + " ";
		}
		return columns;
	}

	private String hbaseColumn(String columnName) {
		if (columnName.indexOf(":") > 0) {
			return columnName;
		}
		return columnName + ":";
	}

}
