package marmot.hadoop.command;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import marmot.hadoop.MarmotHadoopServer;
import marmot.remote.server.GrpcDataSetServiceServant;
import marmot.remote.server.GrpcFileServiceServant;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.NetUtils;
import utils.func.Unchecked;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="marmot_grpc_hdfs_dataset_server",
			parameterListHeading = "Parameters:%n",
			optionListHeading = "Options:%n",
			description="create a remote HDFS DataSetServer (using gRPC)")
public class GrpcHdfsMarmotServerMain extends MarmotHadoopCommand {
	private final static Logger s_logger = LoggerFactory.getLogger(GrpcHdfsMarmotServerMain.class);
	
	private static final int DEFAULT_MARMOT_PORT = 15685;
	
	@Option(names={"-port"}, paramLabel="number", required=false,
			description={"marmot DataSetServer port number"})
	private int m_port = -1;
	
	public static final void main(String... args) throws Exception {
		run(new GrpcHdfsMarmotServerMain(), args);
	}
	
	@Override
	protected void run(MarmotHadoopServer marmot) throws Exception {
		int port = m_port;
		if ( port < 0 ) {
			String portStr = System.getenv("MARMOT_GRPC_PORT");
			port = (portStr != null) ? Integer.parseInt(portStr) : DEFAULT_MARMOT_PORT;
		}
		
		GrpcDataSetServiceServant servant = new GrpcDataSetServiceServant(marmot.getDataSetServer());
		GrpcFileServiceServant fileServant = new GrpcFileServiceServant(marmot.getFileServer());

		Server server = NettyServerBuilder.forPort(port)
											.addService(servant)
											.addService(fileServant)
											.build();
		server.start();

		if ( m_verbose ) {
			System.out.println("use DataSerServer: " + marmot.getDataSetServer());
			System.out.println("use FileServer: " + marmot.getFileServer());
			
			String host = NetUtils.getLocalHostAddress();
			System.out.printf("started: %s[host=%s, port=%d]%n", marmot.getClass().getSimpleName(), host, port);
		}

		getTerminationLockFile()
			.orElse(() -> getHomeDir().map(dir -> new File(dir, ".lock")))
			.ifPresent(lock -> {
				s_logger.info("monitor the termination_lock: {}", lock.getAbsolutePath());
				CompletableFuture.runAsync(Unchecked.sneakyThrow(() -> awaitTermination(server, lock)));
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
