package geom.advanced;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
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
public class SampleEstimateIDW {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "주민/인구밀도_2000";
	private static final String VALUE_COLUMN = "value";
	private static final double RADIUS = 5 * 1000;
	private static final int TOP_K = 10;

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
		
		Plan plan;
		DataSet result;
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:5186");
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		String tempPath = "tmp/points";
		
		plan = marmot.planBuilder("to_point")
						.load(INPUT)
						.centroid("the_geom")
						.project("the_geom, big_sq, value")
						.store(tempPath)
						.build();
		result = marmot.createDataSet(tempPath, plan, GEOMETRY(info), FORCE);
		result.cluster();
		
		plan = marmot.planBuilder("sample_estimate_idw")
						.load(tempPath)
						.estimateIDW("the_geom", tempPath, VALUE_COLUMN, RADIUS,
										TOP_K, "value")
						.store(RESULT)
						.build();
		result = marmot.createDataSet(RESULT, plan, GEOMETRY(info), FORCE);
		
		marmot.deleteDataSet(tempPath);
		
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
