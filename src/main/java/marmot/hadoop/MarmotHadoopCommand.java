package marmot.hadoop;

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
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import picocli.CommandLine;
import picocli.CommandLine.DefaultExceptionHandler;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Help;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
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
public class MarmotHadoopCommand implements PicocliCommand<Configuration> {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotHadoopCommand.class);
	
	private static final String HADOOP_CONFIG = "hadoop-conf";
	private static final String ENV_VAR_HOME = "MARMOT_HOME";
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;

	@Option(names={"-home"}, paramLabel="path", description={"Marmot home directory"})
	@Nullable private File m_homeDir = null;
	@Option(names={"-config"}, paramLabel="path", description={"Marmot config directory"})
	@Nullable private File m_configDir = null;
	private MapReduceMode m_mrMode = MapReduceMode.NONE;
	
	@Option(names={"-lock"}, paramLabel="path", description={"MarmotServer termination-lock file"})
	private String m_lock = null;
	
	@Option(names={"-v"}, description={"verbose"})
	private boolean m_verbose = false;
	
	private Configuration m_conf;		// initial context
	private String[] m_args;
	private String[] m_applArgs;
	
	protected void run(Configuration conf) throws Exception { }
	
	protected static void run(MarmotHadoopCommand cmd) throws Exception {
		CommandLine cmdLine = new CommandLine(cmd).setUnmatchedArgumentsAllowed(true);
		ParseResult parseResult = null;
		try {
			parseResult = cmdLine.parseArgs(cmd.m_args);
			new RunLast().useAnsi(Help.Ansi.OFF).handleParseResult(parseResult);
		}
		catch ( ParameterException e ) {
			DefaultExceptionHandler exceptHandler = new DefaultExceptionHandler();
			exceptHandler.useAnsi(Help.Ansi.OFF);
			exceptHandler.handleParseException(e, cmd.m_args);
		}
		catch ( ExecutionException e ) {
			DefaultExceptionHandler exceptHandler = new DefaultExceptionHandler();
			exceptHandler.useAnsi(Help.Ansi.OFF);
			exceptHandler.handleExecutionException(e, parseResult);
		}
	}
	
	public MarmotHadoopCommand(String... args) {
		m_args = args;
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
		ConfigurationBuilder builder = new ConfigurationBuilder()
											.setMapReduceMode(m_mrMode);
		getConfigDir().transform(builder, (b,d) -> b.setConfigDir(d));
		
		try {
			Tuple<Configuration,String[]> ctx = builder.build(m_args);
			m_conf = ctx._1;
			m_applArgs = ctx._2;
			
			Configuration conf = getInitialContext();
			run(conf);
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}
	
	@Override
	public Configuration getInitialContext() {
		return m_conf;
	}
	
	public FOption<File> getTerminationLockFile() {
		return FOption.ofNullable(m_lock).map(File::new);
	}
	
	public static File getLog4jPropertiesFile() {
		String homeDir = FOption.ofNullable(System.getenv("MARMOT_SERVER_HOME"))
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