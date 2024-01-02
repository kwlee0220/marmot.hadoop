package marmot.hadoop.support;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

import marmot.hadoop.io.MarmotFileException;
import marmot.hadoop.io.MarmotFileNotFoundException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class HdfsPath implements Serializable {
	private static final Logger s_logger = LoggerFactory.getLogger(HdfsPath.class);
	
	private transient Configuration m_conf;
	private transient Path m_path;
	private transient FileSystem m_fs;
	
	public static HdfsPath of(Configuration conf, Path path) {
		return new HdfsPath(conf, null, path);
	}
	
	public static HdfsPath of(FileSystem fs, Path path) {
		return new HdfsPath(null, fs, path);
	}
	
	public static HdfsPath of(Configuration conf, FileStatus fstat) {
		return new HdfsPath(conf, null, fstat);
	}
	
	private HdfsPath(Configuration conf, FileSystem fs, Path path) {
		Utilities.checkArgument(conf != null || fs != null, "conf != null || fs != null");
		Utilities.checkNotNullArgument(path, "path is null");
		
		m_conf = conf != null ? conf : fs.getConf();
		m_fs = fs;
		m_path = path;
	}
	
	private HdfsPath(Configuration conf, FileSystem fs, FileStatus fstat) {
		Utilities.checkArgument(conf != null || fs != null, "conf != null || fs != null");
		Utilities.checkNotNullArgument(fstat, "FileStatus is null");

		m_conf = conf != null ? conf : fs.getConf();
		m_fs = fs;
		m_path = fstat.getPath();
	}
	
	public Configuration getConf() {
		return m_conf;
	}
	
	public Path getPath() {
		return m_path;
	}
	
	public Path getAbsolutePath() throws IOException {
		return m_path.isAbsolute() ? m_path : getFileStatus().getPath();
	}
	
	public String getName() {
		return m_path.getName();
	}
	
	public long getLength() throws IOException {
		return getFileStatus().getLen();
	}
	
	public FileSystem getFileSystem() {
		if ( m_fs == null ) {
			try {
				m_fs = FileSystem.get(m_conf);
			}
			catch ( Exception e ) {
				throw new MarmotFileException("" + e);
			}
		}
		
		return m_fs;
	}

	public FileStatus getFileStatus() throws IOException {
		return getFileSystem().getFileStatus(m_path);
	}
	
	public FOption<HdfsPath> getParent() {
		Path parentPath = m_path.getParent();
		return (parentPath != null)
				? FOption.of(new HdfsPath(m_conf, m_fs, parentPath))
				: FOption.empty();
	}
	
	public HdfsPath child(String name) {
		Utilities.checkNotNullArgument(name, "name is null");
		
		return new HdfsPath(m_conf, m_fs, new Path(m_path, name));
	}

	public boolean exists() {
		try {
			return getFileSystem().exists(m_path);
		}
		catch ( AccessControlException e ) {
			return false;
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}

	public boolean isDirectory() throws IOException {
		return getFileStatus().isDirectory();
	}

	public boolean isFile() throws IOException {
		return getFileStatus().isFile();
	}
	
	public void makeParentDirectory() {
		FOption<HdfsPath> oparent = getParent();
		if ( oparent.isPresent() ) {
			HdfsPath parent = oparent.get();
			if ( !parent.exists() ) {
				parent.mkdir();
			}
		}
	}
	
	public FSDataOutputStream create() {
		try {
			makeParentDirectory();
			return getFileSystem().create(m_path);
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	public FSDataOutputStream create(boolean overwrite, long blockSize) {
		int bufferSz = m_conf.getInt(
			                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
			                    CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
		try {
			makeParentDirectory();
			return getFileSystem().create(m_path, overwrite, bufferSz,
										m_fs.getDefaultReplication(m_path), blockSize);
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	public FSDataOutputStream append() {
		try {
			return getFileSystem().append(m_path);
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	public FSDataInputStream open() {
		try {
			return getFileSystem().open(m_path);
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	/**
	 * 경로에 해당하는 HDFS 파일을 삭제한다.
	 * 여러 이유로 파일 삭제에 실패한 경우는 {@code false}를 반환한다.
	 * 
	 * @return	파일 삭제 여부.
	 */
	public boolean delete() {
		try {
			FileSystem fs = getFileSystem();
			
			if ( !exists() ) {
				return true;
			}
			
			return fs.delete(m_path, true);
		}
		catch ( IOException e ) {
			s_logger.warn("fails delete file: " + this, e);
			return false;
		}
	}
	
	public boolean deleteUpward() {
		if ( !delete() ) {
			return false;
		}

		return getParent().map(HdfsPath::deleteIfEmptyDirectory)
							.getOrElse(true);
	}
	
	private boolean deleteIfEmptyDirectory() {
		try {
			if ( getFileSystem().listStatus(m_path).length == 0 ) {
				if ( !delete() ) {
					return false;
				}
				
				return getParent().map(HdfsPath::deleteIfEmptyDirectory)
									.getOrElse(true);
			}
			else {
				return true;
			}
		}
		catch ( IOException e ) {
			return false;
		}
	}
	
	public boolean mkdir() {
		if ( exists() ) {
			throw new MarmotFileException("already exists: path=" + m_path);
		}
		
		try {
			FOption<HdfsPath> oparent = getParent();
			if ( oparent.isPresent() ) {
				HdfsPath parent = oparent.getUnchecked();
				
				if ( !parent.exists() ) {
					if ( !parent.mkdir() ) {
						return false;
					}
				}
				else if ( !parent.isDirectory() ) {
					throw new MarmotFileException("the parent file is not a directory");
				}
			}
			
			return getFileSystem().mkdirs(m_path);
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	public void moveTo(HdfsPath dst) {
		Utilities.checkNotNullArgument(dst, "dst is null");
		
		if ( !exists() ) {
			throw new MarmotFileException("source file not found: path=" + m_path);
		}
		
		if ( dst.exists() ) {
			throw new MarmotFileException("destination exists: path=" + dst);
		}

		try {
			FOption<HdfsPath> oparent = dst.getParent();
			if ( oparent.isPresent() ) {
				HdfsPath parent = oparent.getUnchecked();
				if ( !parent.exists() ) {
					if ( !parent.mkdir() ) {
						throw new MarmotFileException("fails to create destination's parent directory");
					}
				}
				else if ( !parent.isDirectory() ) {
					throw new MarmotFileException("destination's parent is not a directory: path=" + parent);
				}
			}
			
			if ( !getFileSystem().rename(m_path, dst.m_path) ) {
				throw new MarmotFileException("fails to rename to " + dst);
			}

			// 파일 이동 후, 디렉토리가 비게 되면 해당 디렉토리를 올라가면서 삭제한다.
			getParent().ifPresent(HdfsPath::deleteIfEmptyDirectory);
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	public FStream<HdfsPath> streamChildFiles() {
		try {
			return FStream.of(getFileSystem().listStatus(m_path))
							.map(s -> HdfsPath.of(m_conf, s));
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}

	/**
	 * 주어진 경로에 해당하는 디렉토리에 포함된 모든 일반 파일의 상태 객체의 스트림을 반환한다.
	 * 반환된 스트림을 통해 디렉토리에 (재귀적으로) 포함된 모든 일반 파일의 상태를 접근할 수 있다.
	 * 만일 주어진 경로가 일반 파일인 경우인 경우는 해당 일반 파일의 상태 객체만을 접근하는 스트림이 반환된다.
	 * 
	 * @return	 파일의 상태 객체의 스트림
	 * @throws IOException 
	 */
	public FStream<HdfsPath> walkRegularFileTree() throws IOException {
		return walkTree(getFileSystem(), m_path)
					.filter(IS_REGULAR_FILE)
					.map(fstat -> HdfsPath.of(m_conf, fstat));
	}

	/**
	 * 주어진 경로에 해당하는 디렉토리에 포함된 모든 파일의 상태 객체의 스트림을 반환한다.
	 * 반환된 스트림을 통해 디렉토리에 (재귀적으로) 포함된 모든 파일의 상태를 접근할 수 있다.
	 * 만일 주어진 경로가 파일인 경우인 경우는 해당 파일의 상태 객체만을 접근하는 스트림이 반환된다.
	 * 
	 * @param includeCurrent	스트림에 본 객체 포함 여부
	 * @return	 파일의 상태 객체의 스트림
	 * @throws IOException 
	 */
	public FStream<HdfsPath> walkTree(boolean includeCurrent) throws IOException {
		FStream<FileStatus> strm = walkTree(getFileSystem(), m_path);
		if ( !includeCurrent ) {
			strm = strm.drop(1);
		}
		return strm.map(fstat -> HdfsPath.of(m_conf, fstat));
	}
	
	public static FStream<HdfsPath> walkRegularFileTree(Configuration conf, List<Path> starts) {
		return walkRegularFileTree(getFileSystem(conf), starts);
	}
	
	public static FStream<HdfsPath> walkRegularFileTree(FileSystem fs, Path... starts) {
		return walkRegularFileTree(fs, Arrays.asList(starts));
	}
	
	public static FStream<HdfsPath> walkRegularFileTree(FileSystem fs, List<Path> starts) {
		return FStream.from(starts)
						.map(path -> HdfsPath.of(fs, path))
						.flatMap(path -> {
							try {
								return path.walkRegularFileTree();
							}
							catch ( Exception e ) {
								s_logger.warn("fails to traverse: file=" + path + ", cause=" + e);
								return FStream.empty();
							}
						});
	}
	
	public static FStream<HdfsPath> walkRegularFileTree(List<HdfsPath> starts) {
		return FStream.from(starts)
						.flatMap(path -> {
							try {
								return path.walkRegularFileTree();
							}
							catch ( Exception e ) {
								s_logger.warn("fails to traverse: file=" + path + ", cause=" + e);
								return FStream.empty();
							}
						});
	}

	public static Predicate<FileStatus> IS_REGULAR_FILE = (fstat) -> {
		if ( fstat.isDirectory() ) {
			return false;
		}
		String fname = fstat.getPath().getName();
		if ( fname.startsWith("_") || fname.startsWith(".") ) {
			return false;
		}
		return true;
	};

	public static FileSystem getLocalFileSystem() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", "file:///");
			
			return FileSystem.get(conf);
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}

	public static FileSystem getFileSystem(Configuration conf) {
		try {
			return FileSystem.get(conf);
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}

	public static Configuration getLocalFsConf() {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		
		return conf;
	}
	
	@Override
	public String toString() {
		return m_path.toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		else if ( o == null || getClass() != o.getClass() ) {
			return false;
		}
		
		HdfsPath other = (HdfsPath)o;
		return m_path.equals(other.m_path);
	}
	
	@Override
	public int hashCode() {
		return m_path.hashCode();
	}
	
	private void writeObject(ObjectOutputStream os) throws IOException {
		os.defaultWriteObject();
		
		m_conf.write(os);
		os.writeUTF(m_path.toString());
	}
	
	private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
		is.defaultReadObject();
		
		m_conf = new Configuration();
		m_conf.readFields(is);
		
		m_path = new Path(is.readUTF());
	}
	
	private static FStream<FileStatus> walkTree(FileSystem fs, Path start) {
		try {
			FileStatus current = fs.getFileStatus(start);
			if ( current.isDirectory() ) {
				FStream<FileStatus> children = FStream.of(fs.listStatus(start))
														.flatMap(fstat -> {
															if ( fstat.isDirectory() ) {
																Path path = fstat.getPath();
																return walkTree(fs, path);
															}
															else {
																return FStream.of(fstat);
															}
														});
				return FStream.of(current).concatWith(children);
			}
			else {
				return FStream.of(current);
			}
		}
		catch ( FileNotFoundException e ) {
			throw new MarmotFileNotFoundException("" + start);
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
}
