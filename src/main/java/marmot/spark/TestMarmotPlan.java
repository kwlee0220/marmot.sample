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
	private static final String TEMP = "tmp/tmp";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		PBMarmotSparkSessionClient session = PBMarmotSparkSessionClient.connect("192.168.1.101", 5685);
		
		StopWatch watch = StopWatch.start();
		
		Plan plan;
		plan = marmot.planBuilder("phase_01")
					.load("blockgeo")
					.hashJoin("emdcode", "emdgeo", "emdcode", "BLOCK_CD,param.{sigcode}", INNER_JOIN)
					.hashJoin("sigcode", "siggeo", "sigcode", "BLOCK_CD,param.{sdcode}", INNER_JOIN)
					.hashJoin("sdcode", "sdgeo", "sdcode", "BLOCK_CD,param.{name}", INNER_JOIN)
					.store("tmp/temp", FORCE)
					.build();
//		marmot.execute(plan);
		session.execute(plan);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		plan = marmot.planBuilder("phase_02")
					.load("flow_pop_time")
					.hashJoin("BLOCK_CD", "tmp/temp", "BLOCK_CD", "avg_10tmst,param.{name}",INNER_JOIN)
					.aggregateByGroup(Group.ofKeys("name"), AVG("avg_10tmst"))
					.store(RESULT, FORCE)
					.build();
//		marmot.execute(plan);
		session.execute(plan);
		marmot.deleteDataSet("tmp/temp");
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
