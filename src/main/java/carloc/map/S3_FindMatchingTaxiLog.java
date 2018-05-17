package carloc.map;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S3_FindMatchingTaxiLog {
	private static final String INPUT = Globals.TAXI_LOG_DONG;
	private static final String RESULT = "tmp/matching_taxi_log";
	
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
		
		DataSet input = marmot.getDataSet(INPUT);
		String geomCol = input.getGeometryColumn();
		
		Plan plan;
		plan = marmot.planBuilder("맵_매핑_택시로그_검색")
					.load(INPUT)
					.knnJoin(geomCol, Globals.ROADS, Globals.DISTANCE, 1, "*")
					.store(RESULT)
					.build();
		DataSet result = marmot.createDataSet(RESULT, input.getGeometryColumnInfo(), plan, true);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedTimeString());
	}
}
