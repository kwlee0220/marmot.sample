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
public class B01_SumMonthGasYear {
	private static final String INPUT = Globals.GAS;
	private static final String OUTPUT = String.format("tmp/anyang/gas%d", Globals.YEAR);
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		String planName = String.format("%d년 월별 가스 사용량 합계", Globals.YEAR);
		String filterPred = String.format("year == %d", Globals.YEAR);

		Plan plan;
		plan = Plan.builder(planName)
					.load(INPUT)
					.filter("사용량 >= 0")
					.defineColumn("year:short", "사용년월.substring(0, 4)")
					.filter(filterPred)
					.defineColumn("month:short", "사용년월.substring(4, 6)")
					.aggregateByGroup(Group.ofKeys("pnu,month").workerCount(1), SUM("사용량").as("usage"))
					.project("pnu, month,  usage")
					.store(OUTPUT, FORCE)
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		marmot.close();
	}
}
