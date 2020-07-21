package marmot.hadoop.command;

import marmot.command.DatasetCommands;
import marmot.command.UploadFilesCommand;
import marmot.hadoop.MarmotHadoopServer;
import marmot.hadoop.command.HdfsDataSetMain.Format;
import marmot.hadoop.dataset.HdfsAvroDataSetServer;
import picocli.CommandLine.Command;
import utils.PicocliSubCommand;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="mh_dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="dataset-related commands",
		subcommands = {
			DatasetCommands.ListDataSet.class,
			DatasetCommands.Show.class,
			DatasetCommands.Schema.class,
			DatasetCommands.Move.class,
//			DatasetCommands.SetGcInfo.class,
//			DatasetCommands.AttachGeometry.class,
//			DatasetCommands.Count.class,
//			DatasetCommands.Bind.class,
			DatasetCommands.Delete.class,
			DatasetCommands.Import.class,
			DatasetCommands.Export.class,
			UploadFilesCommand.class,
//			DatasetCommands.Thumbnail.class,
			Format.class,
		})
public class HdfsDataSetMain extends MarmotHadoopCommand {
	public static final void main(String... args) throws Exception {
		run(new HdfsDataSetMain(), args);
	}

	@Override
	protected void run(MarmotHadoopServer marmot) throws Exception { }

	@Command(name="format", description="format the dataset store")
	public static class Format extends PicocliSubCommand<MarmotHadoopServer> {
		@Override
		public void run(MarmotHadoopServer marmot) throws Exception {
			HdfsAvroDataSetServer server = marmot.getDataSetServer();
			HdfsAvroDataSetServer.format(server.getConf());
		}
	}
}
