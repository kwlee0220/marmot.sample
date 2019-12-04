package marmot.spark;

import static marmot.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.optor.JoinOptions;
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
		PBMarmotSparkSessionClient session = PBMarmotSparkSessionClient.connect("192.168.1.101", 5685);
		
		StopWatch watch = StopWatch.start();
		
		Plan plan;
		plan = getPlan(marmot);
//		marmot.execute(plan);
		session.execute(plan);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static final Plan getPlan4(MarmotRuntime marmot) {
		return marmot.planBuilder("test")
					.load("emdgeo")
					.hashJoin("sigcode", "siggeo", "sigcode",
								"*,param.*-{geom,sigcode},param.geom as geom2", JoinOptions.INNER_JOIN)
					.hashJoin("sigcode", "emdgeo", "sigcode", "name,param.{emdcode}", JoinOptions.INNER_JOIN)
					.hashJoin("emdcode", "blockgeo", "emdcode", "name,param.{BLOCK_CD}", JoinOptions.INNER_JOIN)
					.hashJoin("BLOCK_CD", "flow_pop_time2", "BLOCK_CD", "name,param.{avg_10tmst,year}",
							JoinOptions.INNER_JOIN)
					.filter("year == 2015")
					.aggregateByGroup(Group.ofKeys("name,year"), AggregateFunction.AVG("avg_10tmst"))
					.store(RESULT, FORCE)
					.build();
	}
	
	private static final Plan getPlan(MarmotRuntime marmot) {
		return marmot.planBuilder("test")
					.load("sdgeo")
					.hashJoin("sdcode", "siggeo", "sdcode", "name,param.{sigcode}", JoinOptions.INNER_JOIN)
					.hashJoin("sigcode", "emdgeo", "sigcode", "name,param.{emdcode}", JoinOptions.INNER_JOIN)
					.hashJoin("emdcode", "blockgeo", "emdcode", "name,param.{BLOCK_CD}", JoinOptions.INNER_JOIN)
					.hashJoin("BLOCK_CD", "flow_pop_time2", "BLOCK_CD", "name,param.{avg_10tmst,year}",
							JoinOptions.INNER_JOIN)
					.filter("year == 2015")
					.aggregateByGroup(Group.ofKeys("name,year"), AggregateFunction.AVG("avg_10tmst"))
					.store(RESULT, FORCE)
					.build();
	}
	
	private static final Plan getPlan2(MarmotRuntime marmot) {
		return marmot.planBuilder("test2")
					.load("구역/시군구")
					.hashJoin("SIG_CD", "교통/지하철/역사", "SIG_CD",
								"SIG_KOR_NM,param.{the_geom,KOR_SUB_NM}", JoinOptions.INNER_JOIN)
					.store(RESULT, FORCE)
					.build();
	}
	
	private static final Plan getPlan3(MarmotRuntime marmot) {
		return marmot.planBuilder("test3")
					.load("구역/시군구")
					.hashJoin("SIG_CD", "교통/지하철/역사", "SIG_CD",
								"SIG_KOR_NM,param.{the_geom,KOR_SUB_NM}", JoinOptions.INNER_JOIN)
					.store(RESULT, FORCE)
					.build();
	}
}
