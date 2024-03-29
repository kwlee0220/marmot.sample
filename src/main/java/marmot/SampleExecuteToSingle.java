package marmot;

import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExecuteToSingle {
	private static final String INPUT = "교통/지하철/서울역사";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan = Plan.builder("test")
							.load(INPUT)
							.aggregate(AggregateFunction.COUNT())
							.build();
		Long count = marmot.executeToLong(plan).get();
		System.out.println("count=" + count);
	}
}
