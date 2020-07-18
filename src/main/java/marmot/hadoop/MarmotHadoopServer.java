package marmot.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import marmot.MarmotRuntime;
import marmot.hadoop.dataset.HdfsAvroDataSetServer;
import marmot.hadoop.file.HdfsFileServer;
import marmot.hadoop.support.HdfsPath;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotHadoopServer implements MarmotRuntime, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final Configuration m_conf;
	private final FileSystem m_fs;
	private final HdfsAvroDataSetServer m_dsServer;
	private final HdfsFileServer m_fileServer;

	public MarmotHadoopServer(Configuration conf) {
		m_conf = conf;
		m_fs = HdfsPath.getFileSystem(conf);
		
		m_dsServer = new HdfsAvroDataSetServer(conf);
		m_fileServer = new HdfsFileServer(conf, new Path("."));
	}
	
	/**
	 * 하둡 설정을 반환한다.
	 * 
	 * @return	설정 객체.
	 */
	public Configuration getHadoopConfiguration() {
		return m_conf;
	}
	
	/**
	 * HDFS 파일 시스템 객체를 반환한다.
	 * 
	 * @return	HDFS 파일 시스템
	 */
	public FileSystem getHadoopFileSystem() {
		return m_fs;
	}

	@Override
	public HdfsAvroDataSetServer getDataSetServer() {
		return m_dsServer;
	}

	@Override
	public HdfsFileServer getFileServer() {
		return m_fileServer;
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		private final byte[] m_confBytes;
		
		private SerializationProxy(MarmotHadoopServer marmot) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try ( DataOutputStream out = new DataOutputStream(baos) ) {
				marmot.m_conf.write(out);
			}
			catch ( IOException nerverHappens ) { }
			m_confBytes = baos.toByteArray();
		}
		
		private Object readResolve() {
			try ( DataInputStream dis = new DataInputStream(new ByteArrayInputStream(m_confBytes)) ) {
				Configuration conf = new Configuration();
				conf.readFields(dis);
				
				return new MarmotHadoopServer(conf);
			}
			catch ( IOException e ) {
				throw new RuntimeException(e);
			}
		}
	}
}
