package marmot.hadoop.dataset;

import static java.util.concurrent.CompletableFuture.runAsync;
import static utils.func.Unchecked.sneakyThrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import marmot.hadoop.MarmotHadoopCommand;
import marmot.remote.server.GrpcDataSetServiceServant;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.NetUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="marmot_grpc_hdfs_dataset_server",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="create a remote HDFS DataSetServer (using gRPC)")
public class GrpcHdfsDataSetServerMain extends MarmotHadoopCommand {
	private final static Logger s_logger = LoggerFactory.getLogger(GrpcHdfsDataSetServerMain.class);
	
	private static final int DEFAULT_MARMOT_PORT = 15685;
	private static final File STORE_ROOT = new File("/home/kwlee/tmp/datastore");
	
	@Option(names={"-port"}, paramLabel="number", required=false,
			description={"marmot DataSetServer port number"})
	private int m_port = -1;
	
	public static final void main(String... args) throws Exception {
		File propsFile = MarmotHadoopCommand.configureLog4j();
		System.out.printf("loading marmot log4j.properties: %s%n", propsFile.getAbsolutePath());
		
		GrpcHdfsDataSetServerMain cmd = new GrpcHdfsDataSetServerMain(args);
		run(cmd);
	}
	
	GrpcHdfsDataSetServerMain(String[] args) {
		super(args);
	}
	
	@Override
	protected void run(Configuration conf) throws Exception {
		int port = m_port;
		if ( port < 0 ) {
			String portStr = System.getenv("MARMOT_PORT");
			port = (portStr != null) ? Integer.parseInt(portStr) : DEFAULT_MARMOT_PORT;
		}
		
		HdfsAvroDataSetServer dsServer = new HdfsAvroDataSetServer(conf);
		GrpcDataSetServiceServant servant = new GrpcDataSetServiceServant(dsServer);

		Server server = NettyServerBuilder.forPort(port)
											.addService(servant)
											.build();
		server.start();

		String host = NetUtils.getLocalHostAddress();
		System.out.printf("started: MarmotServer[host=%s, port=%d]%n", host, port);

		getTerminationLockFile()
			.orElse(() -> getHomeDir().map(dir -> new File(dir, ".lock")))
			.ifPresent(lock -> {
				s_logger.info("monitor the termination_lock: {}", lock.getAbsolutePath());
				runAsync(sneakyThrow(() -> awaitTermination(server, lock)));
			});
		server.awaitTermination();
	}
	
	private void awaitTermination(Server server, File lockFile) throws IOException, InterruptedException {
		String lockPathStr = lockFile.getAbsolutePath();
		
		WatchService watch = FileSystems.getDefault().newWatchService();
		File parent = lockFile.getParentFile();
		parent.toPath().register(watch, StandardWatchEventKinds.ENTRY_MODIFY);
		
        WatchKey key;
		while ( (key = watch.take()) != null ) {
			for ( WatchEvent<?> ev : key.pollEvents() ) {
				if ( ev.kind() == StandardWatchEventKinds.ENTRY_MODIFY ) {
					Path target = Paths.get(parent.getAbsolutePath(), ev.context().toString());
					if ( lockPathStr.equals(target.toString())) {
						server.shutdown();
						
						s_logger.info("terminating the MarmotServer: lock={}",
										lockFile.getAbsolutePath());
						
						return;
					}
				}
			}
			key.reset();
		}
	}
}
