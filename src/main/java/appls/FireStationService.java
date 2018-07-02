package appls;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

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
public class FireStationService {
	private static final String LAND_USAGE = "토지/용도지역지구";
	private static final String POP = "주민/주거인구100m";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
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
		
		Plan plan;
		DataSet result;
		
		DataSet ds = marmot.getDataSet(LAND_USAGE);
		String geomCol = ds.getGeometryColumn();
		
		plan = marmot.planBuilder("combine")
					.load(LAND_USAGE)
					.filter("lclas_cl=='UQA100'")
					.spatialJoin(geomCol, POP, INTERSECTS,
								"param.{the_geom as the_geom,인구수 as pop}")
					.store(RESULT)
					.build();
		result = marmot.createDataSet(RESULT, ds.getGeometryColumnInfo(), plan, true);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
