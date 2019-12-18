package oldbldr;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1_Blocks {
	private static final String BLOCKS = "구역/지오비전_집계구_Point";
	private static final String EMD = "구역/읍면동";
	private static final String RESULT = "tmp/oldbld/blocks_emd";
	
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
		plan = Plan.builder("읍면동별 집계구 수집")
					.load(BLOCKS)
					.spatialJoin("the_geom", EMD, "param.emd_cd,block_cd")
					.store(RESULT, FORCE)
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
