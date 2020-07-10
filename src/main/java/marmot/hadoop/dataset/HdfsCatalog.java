package marmot.hadoop.dataset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import marmot.dataset.Catalog;
import marmot.dataset.CatalogException;
import marmot.dataset.Catalogs;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HdfsCatalog extends Catalog {
	private static final String PROP_DEF_DATASET_DIR = "marmot.catalog.heap.dir";
	private static final String DEF_DATASET_DIR = "datasets/heap";
	
	@SuppressWarnings("unused")
	private final Configuration m_conf;
	private final Path m_prefixPath;
	
	public HdfsCatalog(Configuration conf) {
		super(getJdbcProcessor(conf));
		
		m_conf = conf;
		
		String pathStr = conf.get(PROP_DEF_DATASET_DIR, DEF_DATASET_DIR);
		m_prefixPath = new Path(pathStr);
	}
	
	@Override
	public String toFilePath(String id) {
		id = Catalogs.normalize(id);
		Path path = new Path(m_prefixPath, id.substring(1));
		return path.toString();
	}
	
	public static HdfsCatalog createCatalog(Configuration conf) {
		JdbcProcessor jdbc = getJdbcProcessor(conf);
		
		createCatalog(jdbc);
		return new HdfsCatalog(conf);
	}
	
	public static void dropCatalog(Configuration conf) {
		JdbcProcessor jdbc = getJdbcProcessor(conf);
		dropCatalog(jdbc);;
	}

	private static JdbcProcessor getJdbcProcessor(Configuration conf) {
		String jdbcString = conf.get("marmot.catalog.jdbc.string");
		if ( jdbcString == null ) {
			throw new CatalogException("fails to get JDBC system: name=marmot.catalog.jdbc.string");
		}
		
		return JdbcProcessor.parseString(jdbcString);
	}
}
