package marmot.geom;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcIntersect {
	private static final String INPUT = "안양대/공간연산/intersect/input";
	private static final String PARAM = "안양대/공간연산/intersect/param";
	private static final String RESULT = "tmp/result";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Plan plan = marmot.planBuilder("sample_intersection_join")
							.load(INPUT)
							.intersectionJoin("the_geom", PARAM, SpatialJoinOptions.EMPTY)
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.FORCE);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
