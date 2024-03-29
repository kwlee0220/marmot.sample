package anyang.energe;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class A01_SumMonthGasUsages {
	private static final String INPUT = Globals.GAS;
	private static final String OUTPUT = "tmp/anyang/gas_by_year";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		Plan plan;
		plan = Plan.builder("연별 가스 사용량 합계")
					.load(INPUT)
					.defineColumn("year:short", "사용년월.substring(0, 4)")
					.update("사용량 = Math.max(사용량, 0)")
					.aggregateByGroup(Group.ofKeys("pnu,year"), SUM("사용량").as("usage"))
					.project("pnu,year,usage")
					.store(OUTPUT, FORCE)
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
		
		marmot.close();
	}
}
