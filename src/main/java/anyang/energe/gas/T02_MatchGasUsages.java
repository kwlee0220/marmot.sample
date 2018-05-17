package anyang.energe.gas;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class T02_MatchGasUsages {
	private static final String CADASTRAL = "구역/연속지적도_2017";
	private static final String GAS = "tmp/anyang/gas_year";
	private static final String OUTPUT = "tmp/anyang/gas_cadastral_centers";
	
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
		
		DataSet ds = marmot.getDataSet(CADASTRAL);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();
		JoinOptions jopts = JoinOptions.INNER_JOIN(17);

		Plan plan;
		plan = marmot.planBuilder("가스 사용량 연속지적도에 매칭")
					.load(CADASTRAL)
					.centroid("the_geom", "the_geom")
					.project("*-{big_sq, big_fx}")
					.join("pnu", GAS, "pnu", "the_geom, param.usage", jopts)
					.store(OUTPUT)
					.build();
		DataSet result = marmot.createDataSet(OUTPUT, info, plan, true);
		result.cluster();
		
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 20);
		
		marmot.disconnect();
	}
}
