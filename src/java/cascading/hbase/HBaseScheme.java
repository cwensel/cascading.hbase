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
 * The HBaseScheme class is a {@link Scheme} subclass. It is used in conjunction with the {@HBaseTap} to
 * allow for the reading and writing of data to and from a HBase cluster.
 * 
 * @see HBaseTap
 */
public class HBaseScheme extends Scheme
{
    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger(HBaseScheme.class);

    /** Field keyFields */
    private Fields keyField;
    /** String familyNames */
    private String[] familyNames;
    /** Field valueFields  */
    private Fields[] valueFields;
    /** String columns */
    private transient String[] columns;
    /** Field fields  */
    private transient byte[][] fields;

    private boolean isFullyQualified = false;

    private static String COLUMN_DELIMITER = ",";
    private static String NAMEVALUE_DELIMITER = "=";

    /**
     * Constructor HBaseScheme creates a new HBaseScheme instance.
     * 
     * @param keyFields
     *            of type Fields
     * @param familyName
     *            of type String
     * @param valueFields
     *            of type Fields
     */
    public HBaseScheme(Fields keyFields, String familyName, Fields valueFields)
    {
        this(keyFields, new String[] { familyName }, Fields.fields(valueFields));
    }

    /**
     * Constructor HBaseScheme creates a new HBaseScheme instance.
     * 
     * @param keyFields
     *            of type Fields
     * @param familyNames
     *            of type String[]
     * @param valueFields
     *            of type Fields[]
     */
    public HBaseScheme(Fields keyFields, String[] familyNames, Fields[] valueFields)
    {
        this.keyField = keyFields;
        //The column Names only holds the family Names.
        this.familyNames = familyNames;
        this.valueFields = valueFields;

        setSourceSink(this.keyField, this.valueFields);

        validate();
    }

    /**
     * Constructor HBaseScheme creates a new HBaseScheme instance using fully qualified column names
     * 
     * @param keyField
     *            of type String
     * @param valueFields
     *            of type Fields
     */
    public HBaseScheme(Fields keyField, Fields valueFields)
    {
        this(keyField, Fields.fields(valueFields));
    }

    /**
     * Constructor HBaseScheme creates a new HBaseScheme instance using fully qualified column names
     * 
     * @param keyField
     *            of type Field
     * @param valueFields
     *            of type Field[]
     */
    public HBaseScheme(Fields keyField, Fields[] valueFields)
    {
        //Set a flag that this is using fully qualified names
        this.isFullyQualified = true;
        this.keyField = keyField;
        this.valueFields = valueFields;

        validate();

        setSourceSink(this.keyField, this.valueFields);

    }

    private void validate()
    {
        if (keyField.size() != 1)
            throw new IllegalArgumentException("may only have one key field, found: " + keyField.print());
    }

    private void setSourceSink(Fields keyFields, Fields[] columnFields)
    {
        Fields allFields = Fields.join(keyFields, Fields.join(columnFields)); // prepend

        setSourceFields(allFields);
        setSinkFields(allFields);
    }

    /**
     * Method getFamilyNames returns the set of familyNames of this HBaseScheme object.
     * 
     * @return the familyNames (type String[]) of this HBaseScheme object.
     */
    public String[] getFamilyNames()
    {
        HashSet<String> familyNameSet = new HashSet<String>();
        if (isFullyQualified) {
            for (String columnName : columns(null, this.valueFields))
            {
                int pos = columnName.indexOf(":");
                familyNameSet.add(hbaseColumn(pos>0?columnName.substring(0, pos):columnName));
            }
        } else {
            for (String familyName : familyNames)
            {
                familyNameSet.add(hbaseColumn(familyName));
            }
        }
        return familyNameSet.toArray(new String[0]);
    }

    public Tuple source(Object key, Object value)
    {
        Tuple result = new Tuple();

        ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
        RowResult row = (RowResult) value;

        result.add(Bytes.toString(keyWritable.get()));

        for (byte[] bytes : getFieldsBytes())
        {
            String fieldName = Bytes.toString(bytes);
            Cell cell = row.get(bytes);
            if (cell != null) {
                result.add(Bytes.toString(cell.getValue()));
            } else {
                boolean found = false;
                String delimitedColumnValue = new String();
                for (byte[] col: row.keySet()) {
                    String column = Bytes.toString(col);
                    if (column.startsWith(fieldName)) {
                        delimitedColumnValue += column;
                        delimitedColumnValue += NAMEVALUE_DELIMITER;
                        delimitedColumnValue += Bytes.toString(row.get(col).getValue());
                        delimitedColumnValue += COLUMN_DELIMITER;
                        found = true;
                    }
                }
                if (!found) {
                    result.add(null);
                } else {
                    result.add(delimitedColumnValue.substring(0, delimitedColumnValue.length()-1));
                }
            }
        }

        return result;
    }

    private byte[][] getFieldsBytes()
    {
        if( fields == null )
          fields = makeBytes( this.familyNames, this.valueFields );
    
        return fields;
    }

    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException
    {
        Tuple key = tupleEntry.selectTuple(keyField);

        byte[] keyBytes = Bytes.toBytes(key.getString(0));
        BatchUpdate batchUpdate = new BatchUpdate(keyBytes);

        for (int i = 0; i < valueFields.length; i++)
        {
            Fields fieldSelector = valueFields[i];
            TupleEntry values = tupleEntry.selectEntry(fieldSelector);

            for (int j = 0; j < values.getFields().size(); j++)
            {
                Fields fields = values.getFields();
                Tuple tuple = values.getTuple();
                if (isFullyQualified) {
                    String col = hbaseColumn(fields.get(j).toString());
                    String val = tuple.getString(j);
                    if (val.indexOf(col) > -1) {  //TODO: need to look at better solution to determine column family with multiple columns
                        String[] columns = val.split(COLUMN_DELIMITER);
                        for (String col_val : columns) {
                            String[] split = col_val.split(NAMEVALUE_DELIMITER);
                            batchUpdate.put(split[0], Bytes.toBytes(split[1]));
                        }
                    } else {
                        batchUpdate.put(col, Bytes.toBytes(val));
                    }
                }
                else
                    batchUpdate.put(hbaseColumn(familyNames[i]) + fields.get(j).toString(), Bytes.toBytes(tuple.getString(j)));

            }
        }

        outputCollector.collect(null, batchUpdate);
    }

    public void sinkInit(Tap tap, JobConf conf) throws IOException
    {
        conf.setOutputFormat(TableOutputFormat.class);
        
        conf.setOutputKeyClass(ImmutableBytesWritable.class);
        conf.setOutputValueClass(BatchUpdate.class);
    }

    public void sourceInit(Tap tap, JobConf conf) throws IOException
    {
        conf.setInputFormat(TableInputFormat.class);

        String columns = getColumns();
        LOG.debug("sourcing from columns: {}", columns);

        conf.set(TableInputFormat.COLUMN_LIST, columns);
    }

    private String getColumns()
    {
        return Util.join(columns(this.familyNames, this.valueFields), " ");
    }

    private String[] columns(String[] familyNames, Fields[] fieldsArray)
    {
        if (columns != null)
            return columns;
        
        int size = 0;

        for (Fields fields : fieldsArray)
            size += fields.size();

        columns = new String[size];

        for (int i = 0; i < fieldsArray.length; i++)
        {
            Fields fields = fieldsArray[i];

            for (int j = 0; j < fields.size(); j++)
                if (isFullyQualified)
                    columns[i + j] = hbaseColumn((String) fields.get(j));
                else
                    columns[i + j] = hbaseColumn(familyNames[i]) + (String) fields.get(j);
        }

        return columns;
    }

    private byte[][] makeBytes( String[] familyNames, Fields[] fieldsArray )
    {
        String[] columns = columns( familyNames, fieldsArray );
        byte[][] bytes = new byte[columns.length][];
    
        for( int i = 0; i < columns.length; i++ )
          bytes[ i ] = Bytes.toBytes( columns[ i ] );
    
        return bytes;
    }

    private String hbaseColumn(String column)
    {
        if (column.indexOf(":") < 0)
            return column + ":";
        return column;

    }

}
