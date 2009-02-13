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
	private String[] familyNames;
	private Fields[] valueFields;

	/**
	 * Constructor HBaseScheme creates a new HBaseScheme instance.
	 * 
	 * @param keyName
	 *            of type String
	 * @param familyName
	 *            of type String
	 */
	public HBaseFullScheme(String keyName, String familyName) {
		this(keyName, new String[] { familyName });
	}

	/**
	 * Constructor HBaseScheme creates a new HBaseScheme instance.
	 * 
	 * @param keyName
	 *            of type String
	 * @param familyNames
	 *            of type String[]
	 */
	public HBaseFullScheme(String keyName, String[] familyNames) {
		this.keyName = keyName;
		this.keyFields = new Fields(keyName);
		this.familyNames = familyNames;

		this.valueFields = new Fields[familyNames.length];
		for (int i = 0; i < familyNames.length; i++) {
			this.valueFields[i] = new Fields(familyNames[i]);
		}

		setSourceSink(this.keyFields, this.valueFields);

	}

	private void setSourceSink(Fields keyFields, Fields[] valueFields) {
		Fields allFields = Fields.join(keyFields, Fields.join(valueFields)); // prepend
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
		return familyNames;
	}

	public Tuple source(Object key, Object value) {
		Tuple result = new Tuple();

		ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
		RowResult row = (RowResult) value;

		result.add(Bytes.toString(keyWritable.get()));

		for (byte[] bytes : row.keySet()) {
			Cell cell = row.get(bytes);
			result.add(Bytes.toString(cell.getValue()));
		}

		return result;
	}

	public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
			throws IOException {
		Tuple key = tupleEntry.selectTuple(keyFields);

		byte[] keyBytes = Bytes.toBytes(key.getString(0));
		BatchUpdate batchUpdate = new BatchUpdate(keyBytes);

		for (int i = 0; i < valueFields.length; i++) {
			Fields fieldSelector = valueFields[i];
			TupleEntry values = tupleEntry.selectEntry(fieldSelector);

			for (int j = 0; j < values.getFields().size(); j++) {
				Fields fields = values.getFields();
				Tuple tuple = values.getTuple();
				batchUpdate.put(hbaseColumn(fields.get(j).toString()), Bytes.toBytes(tuple.getString(j)));
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
		for (String family : familyNames) {
			columns += hbaseColumn(family) + " ";
		}
		return columns;
	}

	private String hbaseColumn(String field) {
		return field + ":";
	}

}
