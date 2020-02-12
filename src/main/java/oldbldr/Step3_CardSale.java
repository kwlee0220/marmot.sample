package oldbldr;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.JoinOptions.INNER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
public class Step3_CardSale {
	private static final String CARD_SALES = "주민/카드매출/월별_시간대/2015";
	private static final String BLOCKS = "tmp/oldbld/blocks_emd";
	private static final String RESULT = "tmp/oldbld/card_sale_emd";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		String sumExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		
		Plan plan;
		plan = Plan.builder("읍면동별 2015년도 카드매출 집계")
					.load(CARD_SALES)
					.defineColumn("sale_amt:double", sumExpr)
					.project("block_cd,sale_amt")
					.aggregateByGroup(Group.ofKeys("block_cd"), SUM("sale_amt").as("sale_amt"))
					.hashJoin("block_cd", BLOCKS, "block_cd", "*,param.{emd_cd}", INNER_JOIN)
					.aggregateByGroup(Group.ofKeys("emd_cd").workerCount(1),
										SUM("sale_amt").as("sale_amt"))
					.store(RESULT, FORCE)
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
