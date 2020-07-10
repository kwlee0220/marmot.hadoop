package marmot.hadoop.dataset;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;

import marmot.avro.AvroRecordReader;
import marmot.hadoop.support.HdfsPath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroHdfsRecordReader extends AvroRecordReader {
	private final HdfsPath m_path;
	
	public AvroHdfsRecordReader(HdfsPath path) {
		m_path = path;
	}
	
	public HdfsPath getPath() {
		return m_path;
	}
	
	@Override
	protected DataFileReader<GenericRecord> getFileReader() throws IOException {
		FsInput input = new FsInput(m_path.getPath(), m_path.getConf());
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		return new DataFileReader<>(input, reader);
	}
}
