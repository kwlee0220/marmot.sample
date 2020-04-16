package demo.policy;


import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.LoadOptions;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step04 {
	static final String INPUT = "구역/연속지적도_2017";
	private static final String PARAM = Step01.RESULT;
	private static final String PARAM2 = Step03.RESULT;
	private static final String RESULT = "tmp/10min/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet result;
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan = Plan.builder("노인복지시설필요지역추출")
						.load(INPUT, LoadOptions.FIXED_MAPPERS)
						.spatialSemiJoin(gcInfo.name(), PARAM, SpatialJoinOptions.NEGATED) // (3) 교차반전
						.arcClip(gcInfo.name(), PARAM2)				// (7) 클립분석
						.store(RESULT, StoreDataSetOptions.FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		System.out.printf("elapsed time=%s (processing)%n", watch.getElapsedMillisString());

		result = marmot.getDataSet(RESULT);
//		result.cluster(ClusterSpatiallyOptions.FORCE);
		
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
	}
}
