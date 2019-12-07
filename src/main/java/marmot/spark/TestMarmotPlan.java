package marmot.spark;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.JoinOptions.INNER_JOIN;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
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
//		PBMarmotSparkSessionClient session = PBMarmotSparkSessionClient.connect("192.168.1.101", 5685);
		
		StopWatch watch = StopWatch.start();
		
		Plan plan;
		plan = getPlan4(marmot);
		marmot.execute(plan);
//		session.execute(plan);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static final Plan getPlan4(MarmotRuntime marmot) {
		return marmot.planBuilder("test")
					.load("flow_pop_time")
					.hashJoin("BLOCK_CD", "blockgeo", "BLOCK_CD", "year,avg_10tmst,param.{emdcode}",
								INNER_JOIN)
					.hashJoin("emdcode", "emdgeo", "emdcode", "year,avg_10tmst,param.{sigcode}", INNER_JOIN)
					.hashJoin("sigcode", "siggeo", "sigcode", "year,avg_10tmst,param.{sdcode}", INNER_JOIN)
					.hashJoin("sdcode", "sdgeo", "sdcode", "year,avg_10tmst,param.{name}", INNER_JOIN)
					.aggregateByGroup(Group.ofKeys("name"), AVG("avg_10tmst"))
					.project("name as c0, avg as m0")
					.store(RESULT, FORCE)
					.build();
	}
	
	private static final Plan getPlan(MarmotRuntime marmot) {
		return marmot.planBuilder("test")
					.load("sdgeo")
					.hashJoin("sdcode", "siggeo", "sdcode", "name,param.{sigcode}", INNER_JOIN)
					.hashJoin("sigcode", "emdgeo", "sigcode", "name,param.{emdcode}", INNER_JOIN)
					.hashJoin("emdcode", "blockgeo", "emdcode", "name,param.{BLOCK_CD}", INNER_JOIN)
					.hashJoin("BLOCK_CD", "flow_pop_time2", "BLOCK_CD", "name,param.{avg_10tmst,year}",
								INNER_JOIN)
					.filter("year == 2015")
					.aggregateByGroup(Group.ofKeys("name,year"), AggregateFunction.AVG("avg_10tmst"))
					.store(RESULT, FORCE)
					.build();
	}
	
	private static final Plan getPlan2(MarmotRuntime marmot) {
		return marmot.planBuilder("test2")
					.load("구역/시군구")
					.hashJoin("SIG_CD", "교통/지하철/역사", "SIG_CD",
								"SIG_KOR_NM,param.{the_geom,KOR_SUB_NM}", INNER_JOIN)
					.store(RESULT, FORCE)
					.build();
	}
	
	private static final Plan getPlan3(MarmotRuntime marmot) {
		return marmot.planBuilder("test3")
					.load("구역/시군구")
					.hashJoin("SIG_CD", "교통/지하철/역사", "SIG_CD",
								"SIG_KOR_NM,param.{the_geom,KOR_SUB_NM}", INNER_JOIN)
					.store(RESULT, FORCE)
					.build();
	}
}
