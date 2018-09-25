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
public class S03_ExportRegisteredBuildings27 {
	private static final String REG_BUILDINGS = "tmp/house/registered_buildings";
	
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
		
		DataSet input = marmot.getDataSet(REG_BUILDINGS);
		Plan plan;
		plan = marmot.planBuilder("export01")
					.load(REG_BUILDINGS)
					.filter("sig_cd.startsWith('27')")
					.project("the_geom,bd_mgt_sn")
					.store(REG_BUILDINGS + "_27")
					.build();
		DataSet ds = marmot.createDataSet(REG_BUILDINGS + "_27", input.getGeometryColumnInfo(), plan, DataSetOption.FORCE);
	}
}
