package anyang.energe.test;

import org.apache.log4j.PropertyConfigurator;

import anyang.energe.Globals;
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
public class ExtractChungBukGas {
	private static final String INPUT = Globals.GAS;
//	private static final String INPUT = "gas";
	private static final String OUTPUT = "tmp/result_2011";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("sum_gas_usages ");
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
		plan = marmot.planBuilder("test")
					.load(INPUT)
					.expand("year:int").set("사용년월.substring(0,4)")
					.filter("year == 2017")
					.project("the_geom,")
					.store(OUTPUT)
					.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, true);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
		
		marmot.disconnect();
	}
}
