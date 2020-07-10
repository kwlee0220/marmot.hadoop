package marmot.hadoop.dataset;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import marmot.RecordSchema;
import marmot.avro.AvroRecordWriter;
import marmot.hadoop.support.HdfsPath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroHdfsRecordWriter extends AvroRecordWriter {
	private final HdfsPath m_path;
	
	public AvroHdfsRecordWriter(HdfsPath path) {
		m_path = path;
	}
	
	public HdfsPath getPath() {
		return m_path;
	}

	@Override
	protected DataFileWriter<GenericRecord> getFileWriter(RecordSchema schema, Schema avroSchema) throws IOException {
		m_path.makeParentDirectory();
		
		GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
		writer.setMeta("marmot_schema", schema.toString());
		getSyncInterval().transform(writer, DataFileWriter::setSyncInterval);
		getCodec().transform(writer, DataFileWriter::setCodec);
		writer.create(avroSchema, m_path.create());
		
		return writer;
	}
}
