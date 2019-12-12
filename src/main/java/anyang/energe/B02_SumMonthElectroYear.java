package anyang.energe;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B02_SumMonthElectroYear {
	private static final String INPUT = Globals.ELECTRO;
	private static final String OUTPUT = String.format("tmp/anyang/electro%d", Globals.YEAR);
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		String planName = String.format("%d년 월별 전기 사용량 합계", Globals.YEAR);
		String filterPred = String.format("year == %d", Globals.YEAR);

		Plan plan;
		plan = marmot.planBuilder(planName)
					.load(INPUT)
					.defineColumn("year:short", "사용년월.substring(0, 4)")
					.filter(filterPred)
					.defineColumn("month:short", "사용년월.substring(4, 6)")
					.update("사용량 = Math.max(사용량, 0)")
					.aggregateByGroup(Group.ofKeys("pnu,month").workerCount(1),
									SUM("사용량").as("usage"))
					.project("pnu, month,  usage")
					.store(OUTPUT, FORCE)
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
		
		marmot.close();
	}
}
