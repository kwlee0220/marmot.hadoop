package marmot.hadoop;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import utils.Utilities;
import utils.func.FOption;


/**
 * Hadoop (HDFS, MapReduce2) 접속을 위한 Configuration 객체 builder 클래스.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConfigurationBuilder {
	private static final Logger s_logger = LoggerFactory.getLogger(ConfigurationBuilder.class);
	
	private static final String[] EMPTY_ARGS = new String[0];
	private static final String HADOOP_CONFIG = "hadoop-conf";
	private static final String ENV_VAR_HOME = "MARMOT_HOME";

	@Nullable private File m_configDir = null;
	private MapReduceMode m_mrMode = MapReduceMode.NONE;
	
	public FOption<File> getConfigDir() {
		return FOption.ofNullable(m_configDir);
	}

	public ConfigurationBuilder setConfigDir(File dir) {
		Utilities.checkArgument(dir.isDirectory(), "ConfigDir is not a directory: " + dir.getAbsolutePath());

		m_configDir = dir;
		return this;
	}
	
	public ConfigurationBuilder setMapReduceMode(MapReduceMode mode) {
		m_mrMode = mode;
		return this;
	}
	
	public Configuration build() throws Exception {
		return build(new Configuration());
	}
	
	public Configuration build(Configuration conf) throws FileNotFoundException {
		Utilities.checkNotNullArgument(m_mrMode != null, "runner mode is not specified");
		
		// Marmot 설정 정보 추가
		InputStream rscIs = readMarmotResource("marmot.xml");
		if ( rscIs == null ) {
			throw new IllegalArgumentException("Marmot resource 'marmot.xml' is not found");
		}
		conf.addResource(rscIs);
		
		// Marmot runner 설정 추가
		switch ( m_mrMode ) {
			case NONE:
			case LOCAL:
			case CLUSTER:
				String rscName = String.format("marmot-mr-%s.xml", m_mrMode.name().toLowerCase());
				rscIs = readMarmotResource(rscName);
				if ( rscIs == null ) {
					throw new IllegalArgumentException("Marmot resource '" + rscName + "' is not found");
				}
				conf.addResource(rscIs);
				break;
			default:
				throw new AssertionError("invalid MapReduce mode: " + m_mrMode);
		}
		
		return conf;
	}
	
	private InputStream readMarmotResource(String name) throws FileNotFoundException {
		FOption<File> oconfigDir = getConfigDir();
		if ( oconfigDir.isPresent() ) {
			return new FileInputStream(new File(oconfigDir.getUnchecked(), name));
		}
		else {
			name = String.format("%s/%s", HADOOP_CONFIG, name);
			return Thread.currentThread().getContextClassLoader()
										.getResourceAsStream(name);
		}
	}
	
	public static File getLog4jPropertiesFile() {
		String homeDir = FOption.ofNullable(System.getenv(ENV_VAR_HOME))
								.getOrElse(() -> System.getProperty("user.dir"));
		return new File(homeDir, "log4j.properties");
	}
	
	public static File configureLog4j() throws IOException {
		File propsFile = getLog4jPropertiesFile();
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		
		Map<String,String> bindings = Maps.newHashMap();
		bindings.put("marmot.home", propsFile.getParentFile().toString());

		String rfFile = props.getProperty("log4j.appender.rfout.File");
		rfFile = StringSubstitutor.replace(rfFile, bindings);
		props.setProperty("log4j.appender.rfout.File", rfFile);
		PropertyConfigurator.configure(props);
		
		return propsFile;
	}
}
