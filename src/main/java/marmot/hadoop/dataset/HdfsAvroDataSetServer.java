package marmot.hadoop.dataset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import marmot.RecordReader;
import marmot.RecordWriter;
import marmot.dataset.AbstractDataSetServer;
import marmot.hadoop.support.HdfsPath;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HdfsAvroDataSetServer extends AbstractDataSetServer {
	private final Configuration m_conf;
	
	public HdfsAvroDataSetServer(Configuration conf) {
		super(new HdfsCatalog(conf));
		
		m_conf = conf;
	}
	
	@Override
	protected RecordReader getRecordReader(String filePath) {
		HdfsPath path = HdfsPath.of(m_conf, new Path(filePath));
		return new AvroHdfsRecordReader(path);
	}

	@Override
	protected RecordWriter getRecordWriter(String filePath) {
		HdfsPath path = HdfsPath.of(m_conf, new Path(filePath));
		return new AvroHdfsRecordWriter(path);
	}
}
