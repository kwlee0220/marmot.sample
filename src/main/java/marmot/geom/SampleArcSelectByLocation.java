package marmot.geom;

import static marmot.optor.StoreDataSetOptions.FORCE;
import static marmot.plan.SpatialJoinOptions.WITHIN_DISTANCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcSelectByLocation {
	private static final String BUILDING = "안양대/공간연산/select/building";
	private static final String RIVER = "안양대/공간연산/select/river";
	private static final String RESULT = "tmp/result";

	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(BUILDING).getGeometryColumnInfo();
		
		Plan plan = Plan.builder("sample_select_by_location")
							.load(BUILDING)
							.spatialSemiJoin(gcInfo.name(), RIVER, WITHIN_DISTANCE(50))
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
