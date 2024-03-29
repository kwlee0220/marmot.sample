package marmot.geom;

import static marmot.optor.StoreDataSetOptions.FORCE;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSpatialSemiJoin {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "POI/주유소_가격";
	private static final String PARAMS = "교통/지하철/역사";

	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		Plan plan = Plan.builder("within_distance")
							.load(INPUT)
							.spatialSemiJoin("the_geom", PARAMS,
											SpatialJoinOptions.WITHIN_DISTANCE(30))
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
	}
}
