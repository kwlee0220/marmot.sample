package oldbldr;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.JoinOptions.FULL_OUTER_JOIN;
import static marmot.optor.JoinOptions.INNER_JOIN;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step5_Result {
	private static final String BLOCKS = "tmp/oldbld/blocks_emd";
	private static final String BUILDINGS = "tmp/oldbld/buildings_emd";
	private static final String CARD_SALE = "tmp/oldbld/card_sale_emd";
	private static final String FLOW_POP = "tmp/oldbld/pop_emd";
	private static final String EMD = "구역/읍면동";
	private static final String RESULT = "tmp/oldbld/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		Plan plan;
		plan = marmot.planBuilder("결과 통합")
					.loadHashJoin(BUILDINGS, "emd_cd", CARD_SALE, "emd_cd",
								"left.*,right.*-{emd_cd}", FULL_OUTER_JOIN(1))
					.hashJoin("emd_cd", FLOW_POP, "emd_cd", "*,param.*-{emd_cd}", FULL_OUTER_JOIN(1))
					.hashJoin("emd_cd", EMD, "emd_cd", "param.the_geom,*", INNER_JOIN(1))
					.store(RESULT)
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(EMD).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		watch.stop();
		
		marmot.deleteDataSet(BLOCKS);
		marmot.deleteDataSet(BUILDINGS);
		marmot.deleteDataSet(CARD_SALE);
		marmot.deleteDataSet(FLOW_POP);

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
