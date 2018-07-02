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
public class S03_SumMonthElectroUsages {
	private static final String INPUT = "기타/건물에너지/전기사용량";
	private static final String OUTPUT = "tmp/anyang/electro_year";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("sum_electro_usages ");
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
		plan = marmot.planBuilder("연별 전기 사용량 합계")
					.load(INPUT)
					.expand("year:short", "year = 사용년월.substring(0, 4)")
					.update("사용량 = Math.max(사용량, 0)")
					.groupBy("고유번호,year")
						.aggregate(SUM("사용량").as("usage"))
					.project("고유번호 as pnu, year,  usage")
					.store(OUTPUT)
					.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, true);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
		
		marmot.disconnect();
	}
}