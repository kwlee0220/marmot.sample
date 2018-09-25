package house.misc;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S02_ExportHouseCadastral27 {
	private static final String HOUSE_CADASTRAL = "tmp/house/house_cadastral";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		DataSet input = marmot.getDataSet(HOUSE_CADASTRAL);
		Plan plan;
		plan = marmot.planBuilder("export01")
					.load(HOUSE_CADASTRAL)
					.filter("pnu.startsWith('27')")
					.store(HOUSE_CADASTRAL + "_27")
					.build();
		DataSet ds = marmot.createDataSet(HOUSE_CADASTRAL + "_27", input.getGeometryColumnInfo(),
											plan, DataSetOption.FORCE);
	}
}
