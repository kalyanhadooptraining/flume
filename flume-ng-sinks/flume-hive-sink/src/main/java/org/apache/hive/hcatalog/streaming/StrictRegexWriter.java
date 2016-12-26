package org.apache.hive.hcatalog.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.data.JsonSerDe;

public class StrictRegexWriter extends AbstractRecordWriter {
	private RegexSerDe serde;

	private String regex;
	private ArrayList<String> tableColumns;

	public StrictRegexWriter(HiveEndPoint endPoint) throws ConnectionError, SerializationError, StreamingException {
		super(endPoint, null);
	}

	public StrictRegexWriter(HiveEndPoint endPoint, HiveConf conf)
			throws ConnectionError, SerializationError, StreamingException {
		super(endPoint, conf);
	}

	public StrictRegexWriter(String regex, HiveEndPoint endPoint, HiveConf conf)
			throws ConnectionError, SerializationError, StreamingException {
		super(endPoint, conf);
		this.tableColumns = getCols(this.tbl);
		this.regex = regex;
	}

	protected ArrayList<String> getTableColumns() {
		return this.tableColumns;
	}

	private ArrayList<String> getCols(Table table) {
		List<FieldSchema> cols = table.getSd().getCols();
		ArrayList colNames = new ArrayList(cols.size());
		for (FieldSchema col : cols) {
			colNames.add(col.getName().toLowerCase());
		}
		return colNames;
	}

	@Override
	SerDe getSerde() throws SerializationError {
		if (this.serde != null) {
			return this.serde;
		}
		this.serde = createSerde(this.tbl, this.conf);
		return this.serde;
	}

	@Override
	public void write(long transactionId, byte[] record) throws StreamingIOFailure, SerializationError {
		try {
			Object encodedRow = encode(record);
			this.updater.insert(transactionId, encodedRow);
		} catch (IOException e) {
			throw new StreamingIOFailure("Error writing record in transaction(" + transactionId + ")", e);
		}
	}

	private RegexSerDe createSerde(Table tbl, HiveConf conf) throws SerializationError {
		try {
			Properties tableProps = MetaStoreUtils.getTableMetadata(tbl);
			tableProps.setProperty("input.regex", regex);
			tableProps.setProperty("columns", StringUtils.join(tableColumns, ","));
			RegexSerDe serde = new RegexSerDe();
			SerDeUtils.initializeSerDe(serde, conf, tableProps, null);
			return serde;
		} catch (SerDeException e) {
			throw new SerializationError("Error initializing serde " + JsonSerDe.class.getName(), e);
		}
	}

	private Object encode(byte[] utf8StrRecord) throws SerializationError {
		try {
			Text blob = new Text(utf8StrRecord);
			return this.serde.deserialize(blob);
		} catch (SerDeException e) {
			throw new SerializationError("Unable to convert byte[] record into Object", e);
		}
	}
}