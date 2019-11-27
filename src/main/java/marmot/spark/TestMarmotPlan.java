package marmot.spark;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.JoinOptions.INNER_JOIN;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.remote.protobuf.PBMarmotSparkSessionClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestMarmotPlan {
	private static final String RESULT = "tmp/result";
	private static final String TEMP = "tmp/temp";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		PBMarmotSparkSessionClient session = PBMarmotSparkSessionClient.connect("192.168.1.108", 5685);
		
		StopWatch watch = StopWatch.start();
		
		Plan plan;
		plan = marmot.planBuilder("phase_01")
					.load("sdgeo")
					.hashJoin("sdcode", "siggeo", "sdcode", "name,param.{sigcode}", INNER_JOIN)
					.hashJoin("sigcode", "emdgeo", "sigcode", "name,param.{emdcode}", INNER_JOIN)
					.hashJoin("emdcode", "blockgeo", "emdcode", "name,param.{BLOCK_CD}", INNER_JOIN)
					.store(TEMP, FORCE)
					.build();
		marmot.execute(plan);
//		session.execute(plan);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		plan = marmot.planBuilder("phase_02")
					.loadHashJoin("flow_pop_time2", "BLOCK_CD", TEMP, "BLOCK_CD",
									"left.avg_10tmst,right.name", INNER_JOIN)
					.aggregateByGroup(Group.ofKeys("name"), AVG("avg_10tmst"))
					.store(RESULT, FORCE)
					.build();
		marmot.execute(plan);
//		session.execute(plan);
		marmot.deleteDataSet(TEMP);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
