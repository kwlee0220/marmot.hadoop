package marmot.hadoop.file;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.google.common.io.ByteStreams;

import marmot.file.FileServer;
import marmot.hadoop.support.HdfsPath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HdfsFileServer implements FileServer {
	private final Configuration m_conf;
	private final Path m_root;
	
	public HdfsFileServer(Configuration conf, Path root) {
		m_conf = conf;
		m_root = root;
	}

	@Override
	public InputStream readFile(String path) {
		HdfsPath hpath = HdfsPath.of(m_conf, new Path(m_root, path));
		
		return hpath.open();
	}

	@Override
	public long writeFile(String path, InputStream is) throws IOException {
		HdfsPath hpath = HdfsPath.of(m_conf, new Path(m_root, path));
		
		try ( FSDataOutputStream out = hpath.create() ) {
			return ByteStreams.copy(is, out);
		}
	}

	@Override
	public boolean deleteFile(String path) {
		HdfsPath hpath = HdfsPath.of(m_conf, new Path(m_root, path));
		
		return hpath.delete();
	}
}
