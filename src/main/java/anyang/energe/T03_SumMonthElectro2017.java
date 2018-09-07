package anyang.energe;

import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

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
public class T03_SumMonthElectro2017 {
	private static final String INPUT = Globals.ELECTRO;
	private static final String OUTPUT = "tmp/anyang/electro2017";
	
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
		plan = marmot.planBuilder("2017년 월별 전기 사용량 합계")
					.load(INPUT)
					.expand("year:short").set("year = 사용년월.substring(0, 4)")
					.filter("year == 2017")
					.expand("month:short").set("month = 사용년월.substring(4, 6)")
					.update("사용량 = Math.max(사용량, 0)")
					.groupBy("고유번호,month")
						.workerCount(1)
						.aggregate(SUM("사용량").as("usage"))
					.project("고유번호 as pnu, month,  usage")
					.store(OUTPUT)
					.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, true);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
		
		marmot.disconnect();
	}
}
