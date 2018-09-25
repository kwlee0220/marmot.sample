package carloc.map;

import org.apache.log4j.PropertyConfigurator;

import carloc.Globals;
import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
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
public class S1_MapMatchingTaxiLog3 {
	private static final String INPUT = Globals.TAXI_LOG;
	private static final String RESULT = Globals.TAXI_LOG_MAP;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("map_matching_taxi_log ");
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
		GeometryColumnInfo info = input.getGeometryColumnInfo();
		String geomCol = info.name();
		
		String script = String.format("%s = ST_ClosestPointOnLine(%s, line)", geomCol, geomCol);
		
		Plan plan;
		plan = marmot.planBuilder("택시로그_맵_매핑")
					.load(INPUT)
					.knnJoin(geomCol, Globals.ROADS_IDX, 1, Globals.DISTANCE,
							"*,param.{the_geom as link_geom, link_id, sub_link_no}")
//					.update(script)
					.store(RESULT)
					.build();
		DataSet result = marmot.createDataSet(RESULT, info, plan, DataSetOption.FORCE);
		watch.stop();

		SampleUtils.printPrefix(result, 10);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
