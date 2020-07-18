package marmot.hadoop.dataset;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.RecordReader;
import marmot.RecordSchema;
import marmot.RecordStream;
import marmot.RecordStreamException;
import marmot.avro.AvroUtils;
import marmot.hadoop.support.HdfsPath;
import marmot.stream.MultiSourcesRecordStream;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPathsAvroReader implements RecordReader {
	private static final Logger s_logger = LoggerFactory.getLogger(MultiPathsAvroReader.class);
	
	private final HdfsPath m_start;
	private final RecordSchema m_schema;
	private final Schema m_avroSchema;
	
	public static MultiPathsAvroReader scan(HdfsPath start) {
		return new MultiPathsAvroReader(start);
	}
	
	private MultiPathsAvroReader(HdfsPath start) {
		m_start = start;
		
		m_avroSchema = readAvroSchema(start);
		m_schema = AvroUtils.toRecordSchema(m_avroSchema);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(FStream.from(collectPaths(m_start)));
	}
	
	@Override
	public String toString() {
		return String.format("%s[start=%s]", getClass().getSimpleName(), m_start);
	}
	
	class StreamImpl extends MultiSourcesRecordStream<HdfsPath> {
		StreamImpl(FStream<HdfsPath> paths) {
			super(paths, m_schema);
		}

		@Override
		protected RecordStream read(HdfsPath path) throws RecordStreamException {
			return new AvroHdfsRecordReader(path, MultiPathsAvroReader.this.m_schema, m_avroSchema).read();
		}
	}

	public static List<HdfsPath> collectPaths(HdfsPath start) {
		Utilities.checkNotNullArgument(start, "start is null");
		
		try {
			List<HdfsPath> paths = start.walkRegularFileTree()
										.filter(hpath -> hpath.getName().endsWith(".avro"))
										.toList();
			s_logger.debug("loading paths: from={}, npaths={}", start, paths.size());
			
			return paths;
		}
		catch ( IOException e ) {
			throw new RecordStreamException("fails to collect paths: start=" + start, e);
		}
	}
	
	private static Schema readAvroSchema(HdfsPath path) {
		HdfsPath schemaPath = path.child("_schema.avsc");
		try ( InputStream is = schemaPath.open() ) {
			Schema.Parser parser = new Schema.Parser();
			return parser.parse(is);
		}
		catch ( IOException e ) {
			throw new RecordStreamException("fails to read avro schema file: " + schemaPath, e);
		}
	}
}
