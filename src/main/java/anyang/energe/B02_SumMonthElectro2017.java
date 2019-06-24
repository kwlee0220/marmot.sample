package anyang.energe;

import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B02_SumMonthElectro2017 {
	private static final String INPUT = Globals.ELECTRO;
	private static final String OUTPUT = "tmp/anyang/electro2017";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		Plan plan;
		plan = marmot.planBuilder("2017년 월별 전기 사용량 합계")
					.load(INPUT)
					.defineColumn("year:short", "사용년월.substring(0, 4)")
					.filter("year == 2017")
					.defineColumn("month:short", "사용년월.substring(4, 6)")
					.update("사용량 = Math.max(사용량, 0)")
					.aggregateByGroup(Group.ofKeys("pnu,month").workerCount(1),
									SUM("사용량").as("usage"))
					.project("pnu, month,  usage")
					.store(OUTPUT)
					.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, StoreDataSetOptions.create().force(true));
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
		
		marmot.disconnect();
	}
}
