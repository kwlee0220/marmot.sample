package oldbldr;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.JoinOptions.INNER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
public class Step4_Pop {
	private static final String FLOW_POP = "주민/유동인구/월별_시간대/2015";
	private static final String BLOCKS = "tmp/oldbld/blocks_emd";
	private static final String RESULT = "tmp/oldbld/pop_emd";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		String avgExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.collect(Collectors.joining("+", "(", ")"));
		avgExpr = String.format("avg = %s / 24;", avgExpr);
		
		Plan plan;
		plan = Plan.builder("읍면동별 2015년도 유동인구 집계")
					.load(FLOW_POP)
					.defineColumn("avg:double", avgExpr)
					.project("block_cd,avg")
					.aggregateByGroup(Group.ofKeys("block_cd"), AVG("avg").as("avg"))
					.hashJoin("block_cd", BLOCKS, "block_cd", "*,param.{emd_cd}", INNER_JOIN)
					.aggregateByGroup(Group.ofKeys("emd_cd").workerCount(1), AVG("avg").as("pop_avg"))
					.store(RESULT, FORCE)
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
