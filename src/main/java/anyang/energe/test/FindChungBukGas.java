package anyang.energe.test;

import org.apache.log4j.PropertyConfigurator;

import anyang.energe.Globals;
import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindChungBukGas {
	private static final String INPUT = Globals.GAS;
//	private static final String INPUT = "gas";
	private static final String OUTPUT = "tmp/result";
	
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
					.filter("고유번호.startsWith('43')")
					.filter("사용량 > 0")
					.expand1("sido", DataType.STRING, "고유번호.substring(0,2)")
					.expand1("year", DataType.INT, "사용년월.substring(0,4)")
					.join("고유번호", Globals.CADASTRAL, "pnu", "*,param.the_geom",
							JoinOptions.LEFT_OUTER_JOIN())
					.filter("the_geom == null")
//					.groupBy("year")
//						.workerCount(1)
//						.aggregate(SUM("사용량"), COUNT(), AVG("사용량"))
					.store(OUTPUT)
					.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, DataSetOption.FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
		
		marmot.disconnect();
	}
}
