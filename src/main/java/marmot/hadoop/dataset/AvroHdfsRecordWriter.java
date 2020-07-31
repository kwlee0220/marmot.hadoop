package marmot.hadoop.dataset;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordSchema;
import marmot.avro.AvroRecordWriter;
import marmot.avro.AvroUtils;
import marmot.hadoop.support.HdfsPath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroHdfsRecordWriter extends AvroRecordWriter {
	private static final Logger s_logger = LoggerFactory.getLogger(AvroHdfsRecordWriter.class);
	
	private final HdfsPath m_path;
	private final RecordSchema m_schema;
	private final Schema m_avroSchema;
	
	public AvroHdfsRecordWriter(HdfsPath path, RecordSchema schema, Schema avroSchema) {
		m_path = path;
		m_schema = schema;
		m_avroSchema = avroSchema;
		
		setLogger(s_logger);
	}
	
	public AvroHdfsRecordWriter(HdfsPath path, RecordSchema schema) {
		this(path, schema, AvroUtils.toSchema(schema));
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public HdfsPath getPath() {
		return m_path;
	}

	@Override
	protected DataFileWriter<GenericRecord> getFileWriter() throws IOException {
		m_path.makeParentDirectory();
		
		GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(m_avroSchema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
		writer.setMeta("marmot_schema", m_schema.toString());
		getSyncInterval().transform(writer, DataFileWriter::setSyncInterval);
		getCodec().transform(writer, DataFileWriter::setCodec);
		writer.create(m_avroSchema, m_path.create());
		
		return writer;
	}
	
	@Override
	public String toString() {
		return String.format("AvroHdfsWriter[%s]", m_path);
	}
}
