package geom;

import static marmot.optor.geo.SpatialRelation.WITHIN_DISTANCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSpatialSemiJoin {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "POI/주유소_가격";
	private static final String PARAMS = "교통/지하철/역사";

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
		
		Plan plan = marmot.planBuilder("within_distance")
								.load(INPUT)
								.spatialSemiJoin("the_geom", PARAMS, WITHIN_DISTANCE(30), false)
								.storeMarmotFile(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
