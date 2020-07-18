package marmot.hadoop.dataset;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import marmot.RecordStream;
import marmot.avro.AvroUtils;
import marmot.dataset.AbstractDataSet;
import marmot.dataset.AbstractDataSetServer;
import marmot.dataset.Catalog;
import marmot.dataset.CatalogException;
import marmot.dataset.Catalogs;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetException;
import marmot.dataset.DataSetExistsException;
import marmot.dataset.DataSetInfo;
import marmot.hadoop.support.HdfsPath;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HdfsAvroDataSetServer extends AbstractDataSetServer {
	private static final String PROP_DEF_DATASET_DIR = "marmot.catalog.heap.dir";
	private static final String DEF_DATASET_DIR = "datasets/heap";
	
	private final Configuration m_conf;
	private final HdfsPath m_root;
	
	public HdfsAvroDataSetServer(Configuration conf) {
		super(new Catalog(getJdbcProcessor(conf)));
		
		m_conf = conf;
		m_root = HdfsPath.of(conf, new Path(conf.get(PROP_DEF_DATASET_DIR, DEF_DATASET_DIR)));
	}
	
	public Configuration getConf() {
		return m_conf;
	}
	
	public static HdfsAvroDataSetServer format(Configuration conf) {
		drop(conf);
		return create(conf);
	}
	
	public static HdfsAvroDataSetServer create(Configuration conf) {
		JdbcProcessor jdbc = getJdbcProcessor(conf);
		Catalog.createCatalog(jdbc);

		return new HdfsAvroDataSetServer(conf);
	}
	
	public static void drop(Configuration conf) {
		JdbcProcessor jdbc = getJdbcProcessor(conf);
		Catalog.dropCatalog(jdbc);

		Path root = new Path(conf.get(PROP_DEF_DATASET_DIR, DEF_DATASET_DIR));
		HdfsPath.of(conf, root).delete();
	}
	
	@Override
	public DataSet createDataSet(DataSetInfo dsInfo, boolean force) throws DataSetExistsException {
		HdfsDataSet ds = (HdfsDataSet)super.createDataSet(dsInfo, force);
		HdfsPath path = ds.getHdfsPath();
		
		if ( force && path.exists() ) {
			path.delete();
		}
		
		Schema avroSchema = AvroUtils.toSchema(dsInfo.getRecordSchema());
		HdfsPath schemaPath = path.child("_schema.avsc");
		try ( PrintWriter pw = new PrintWriter(new OutputStreamWriter(schemaPath.create())) ) {
			pw.println(avroSchema.toString(true));
			
			return ds;
		}
	}
	
	@Override
	public String getDataSetUri(String dsId) {
		try {
			dsId = Catalogs.normalize(dsId);
			return new Path(m_root.getFileStatus().getPath(), dsId.substring(1)).toString();
		}
		catch ( IOException e ) {
			throw new IllegalArgumentException("dsid=" + dsId);
		}
	}

	@Override
	protected DataSet toDataSet(DataSetInfo info) {
		return new HdfsDataSet(this, info);
	}

	public static JdbcProcessor getJdbcProcessor(Configuration conf) {
		String jdbcString = conf.get("marmot.catalog.jdbc.string");
		if ( jdbcString == null ) {
			throw new CatalogException("fails to get JDBC system: name=marmot.catalog.jdbc.string");
		}
		
		return JdbcProcessor.parseString(jdbcString);
	}

	public class HdfsDataSet extends AbstractDataSet<HdfsAvroDataSetServer> {
		public HdfsDataSet(HdfsAvroDataSetServer server, DataSetInfo info) {
			super(server, info);
		}
		
		public HdfsPath getHdfsPath() {
			return m_root.child(m_info.getId().substring(1));
		}

		@Override
		public RecordStream read() {
			HdfsPath path = getHdfsPath();
			return MultiPathsAvroReader.scan(path).read();
		}

		@Override
		public long write(RecordStream stream) {
			HdfsPath path = getHdfsPath();
			HdfsPath partPath = path.child(UUID.randomUUID().toString() + ".avro");
			return new AvroHdfsRecordWriter(partPath).write(stream);
		}
	
		@Override
		public long getLength() {
			try {
				return getHdfsPath().getLength();
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to get length: ds=" + this, e);
			}
		}
	}
}
