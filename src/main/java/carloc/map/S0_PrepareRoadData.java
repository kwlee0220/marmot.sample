package carloc.map;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S0_PrepareRoadData {
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
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		DataSet input = marmot.getDataSet(Globals.ROADS);
		String geomCol = input.getGeometryColumn();
		
		Plan plan;
		plan = marmot.planBuilder("도로 경로 simplication")
					.load(Globals.ROADS)
					.flattenGeometry(geomCol, DataType.LINESTRING)
					.breakLineString(geomCol)
					.store(Globals.ROADS_IDX)
					.build();
		DataSet result = marmot.createDataSet(Globals.ROADS_IDX, input.getGeometryColumnInfo(),
												plan, true);
		System.out.printf("elapsed=%s (simplification)%n", watch.getElapsedTimeString());
		
		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed=%s%n", watch.getElapsedTimeString());
	}
}
