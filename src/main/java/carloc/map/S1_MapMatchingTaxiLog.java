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
public class S1_MapMatchingTaxiLog {
	private static final String INPUT = Globals.TAXI_LOG;
	private static final String RESULT = Globals.RESULT;
	
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
		
		String script = String.format("%s = ST_ClosestPointOnLine(%s, line)", geomCol, geomCol);
		
		Plan plan;
		plan = marmot.planBuilder("택시로그_맵_매핑")
					.load(INPUT)
					.knnJoin(geomCol, Globals.ROADS_IDX, Globals.DISTANCE, 1,
							"*,param.{the_geom as line, link_id}")
					.update(script)
					.store(RESULT)
					.build();
		DataSet result = marmot.createDataSet(RESULT, input.getGeometryColumnInfo(), plan, true);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedTimeString());
	}
}
