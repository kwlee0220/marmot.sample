package anyang.energe;

import org.apache.log4j.PropertyConfigurator;

import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B09_SplitGasMap2017 {
	private static final String INPUT = "tmp/anyang/map_gas2017";
	private static final String OUTPUT = "tmp/anyang/map_gas2017_splits";
	
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
		
		GeometryColumnInfo info = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("2017 가스사용량 연속지적도 매칭 분할")
						.load(INPUT)
						.defineColumn("sido:string", "pnu.substring(0, 2)")
						.groupBy("sido")
							.storeEachGroup(OUTPUT, StoreDataSetOptions.create().geometryColumnInfo(info))
						.build();
		
		marmot.deleteDir(OUTPUT);
		marmot.execute(plan);
		watch.stop();

		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		marmot.disconnect();
	}
}
