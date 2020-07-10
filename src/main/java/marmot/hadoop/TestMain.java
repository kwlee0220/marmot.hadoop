package marmot.hadoop;

import org.apache.hadoop.conf.Configuration;

import marmot.RecordSchema;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetInfo;
import marmot.dataset.DataSetServer;
import marmot.hadoop.dataset.HdfsAvroDataSetServer;
import marmot.hadoop.dataset.HdfsCatalog;
import marmot.type.DataType;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="marmot_spark_session",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="create a MarmotSparkSession")
public class TestMain extends MarmotHadoopCommand {
	@Override
	protected void run(Configuration conf) throws Exception {
		HdfsCatalog.dropCatalog(conf);
		HdfsCatalog catalog = HdfsCatalog.createCatalog(conf);
		
		DataSetServer server = new HdfsAvroDataSetServer(conf);
		
		boolean done;
		DataSetInfo info, info2;
		DataSet ds;
		RecordSchema schema = RecordSchema.builder().addColumn("id", DataType.STRING).build();
		
		info = new DataSetInfo("A", schema);
		ds = server.createDataSet(info, true);
		
		info = new DataSetInfo("B", schema);
		ds = server.createDataSet(info, true);
		
		info = new DataSetInfo("/A/A", schema);
		ds = server.createDataSet(info, true);
		
		info = new DataSetInfo("/A/A/A", schema);
		ds = server.createDataSet(info, true);
		
		info = new DataSetInfo("/A/B/A", schema);
		ds = server.createDataSet(info, true);
		
		info = new DataSetInfo("/A/B/B", schema);
		ds = server.createDataSet(info, true);
		
		FStream.from(server.getDataSetAll()).forEach(System.out::println);
		System.out.println("--------------------------------");
		FStream.from(server.getDataSetAllInDir("A", false)).map(DataSet::getId).forEach(System.out::println);
		System.out.println("--------------------------------");
		FStream.from(server.getDataSetAllInDir("A", true)).map(DataSet::getId).forEach(System.out::println);
		System.out.println("--------------------------------");
		
		server.moveDataSet("/A/A/A", "/A/B/C");
		FStream.from(server.getDataSetAllInDir("A", true)).map(DataSet::getId).forEach(System.out::println);
		System.out.println("--------------------------------");
		
		server.moveDir("/A/B", "/A/C");
		FStream.from(server.getDataSetAllInDir("A", true)).map(DataSet::getId).forEach(System.out::println);
		System.out.println("--------------------------------");
		
		done = catalog.deleteDataSetInfo("A/C");
		System.out.println("done (must false): " + done);
		
		done = catalog.deleteDataSetInfo("A/A");
		System.out.println("done (must true): " + done);
		
		done = catalog.deleteDataSetInfo("A/B");
		System.out.println("done (must false): " + done);
		
		int cnt = catalog.deleteDir("A/B");
		System.out.println("count (must 3): " + cnt);
		
//		FSDataInputStream is = HdfsPath.of(conf, new Path("log4j_marmot.properties")).open();
//		System.out.println(IOUtils.toString(is, StandardCharsets.UTF_8));
	}

	public static final void main(String... args) throws Exception {
		MarmotHadoopCommand.configureLog4j();

		TestMain cmd = new TestMain();
		try {
			CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF);
		}
		finally {
//			cmd.getInitialContext().shutdown();
		}
	}
}
