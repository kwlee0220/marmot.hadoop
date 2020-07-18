package marmot.hadoop.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.hadoop.ConfigurationBuilder;
import marmot.hadoop.MapReduceMode;
import marmot.hadoop.MarmotHadoopServer;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.RunLast;
import picocli.CommandLine.Spec;
import utils.PicocliCommand;
import utils.UsageHelp;
import utils.func.FOption;
import utils.func.Tuple;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MarmotHadoopCommand implements PicocliCommand<MarmotHadoopServer> {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotHadoopCommand.class);
	
	private static final String HADOOP_CONFIG = "hadoop-conf";
	private static final String ENV_VAR_HOME = "MARMOT_HADOOP_HOME";
	
	@Spec protected CommandSpec m_spec;
	@Mixin private UsageHelp m_help;

	@Option(names={"--home"}, paramLabel="path", description={"Marmot home directory"})
	@Nullable private File m_homeDir = null;
	@Option(names={"--config"}, paramLabel="path", description={"Marmot config directory"})
	@Nullable private File m_configDir = null;
	private MapReduceMode m_mrMode = MapReduceMode.LOCAL;
	
	@Option(names={"--lock"}, paramLabel="path", description={"MarmotServer termination-lock file"})
	private String m_lock = null;
	
	@Option(names={"-v"}, description={"verbose"})
	private boolean m_verbose = false;
	
	@Nullable private Configuration m_initConf;
	@Nullable private MarmotHadoopServer m_marmot;
	
	protected abstract void run(MarmotHadoopServer marmot) throws Exception;

	public static final void run(MarmotHadoopCommand cmd, String... args) throws Exception {
		File propsFile = configureLog4j();
		
//		Tuple<Configuration, String[]> ctx = getInitialConfiguration(args);
//		cmd.m_initConf = ctx._1;
		cmd.m_initConf = new Configuration();
		new CommandLine(cmd).parseWithHandler(new RunLast(), System.err, args);
	}

	@Option(names={"-mr"}, paramLabel="mode",
			description={"MapReduce-mode ('none', 'local', 'cluster'"})
	public void setMrMode(String mode) {
		m_mrMode = MapReduceMode.valueOf(mode.toUpperCase());
	}
	
	public FOption<File> getHomeDir() {
		Supplier<FOption<File>> suppl = () -> FOption.ofNullable(System.getenv(ENV_VAR_HOME))
														.map(File::new);
		return FOption.ofNullable(m_homeDir)
						.orElse(suppl);
	}
	
	public FOption<File> getConfigDir() {
		Supplier<FOption<File>> suppl  = () -> getHomeDir().map(dir -> new File(dir, HADOOP_CONFIG));
		return FOption.ofNullable(m_configDir)
						.orElse(suppl);
	}
	
	@Override
	public void run() {
		try {
			MarmotHadoopServer marmot = getInitialContext();
			run(marmot);
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}
	
	public MarmotHadoopServer getInitialContext() throws Exception {
		if ( m_marmot == null ) {
			if ( m_initConf == null ) {
				throw new IllegalStateException("base configuration is missing");
			}
			
			ConfigurationBuilder builder = new ConfigurationBuilder()
												.setMapReduceMode(m_mrMode);
			getConfigDir().transform(builder, (b,d) -> b.setConfigDir(d));
			Configuration conf = builder.build(m_initConf);
			
			m_marmot = new MarmotHadoopServer(conf);
		}
		
		return m_marmot;
	}
	
	public FOption<File> getTerminationLockFile() {
		return FOption.ofNullable(m_lock).map(File::new);
	}
	
	public static File getLog4jPropertiesFile() {
		String homeDir = FOption.ofNullable(System.getenv("MARMOT_HADOOP_HOME"))
								.getOrElse(() -> System.getProperty("user.dir"));
		return new File(homeDir, "log4j.properties");
	}
	
	public static File configureLog4j() throws IOException {
		File propsFile = getLog4jPropertiesFile();
		
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
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
		
		return propsFile;
	}
	
	private static Tuple<Configuration,String[]> getInitialConfiguration(String... args) throws Exception {
		Driver driver = new Driver();
		ToolRunner.run(driver, args);
		
		return Tuple.of(driver.getConf(), driver.m_args);
	}
	private static class Driver extends Configured implements Tool {
		private String[] m_args;
		
		@Override
		public int run(String[] args) throws Exception {
			m_args = args;
			return 0;
		}
	}
}