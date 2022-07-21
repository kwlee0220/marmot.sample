package marmot.geom;

import static marmot.optor.StoreDataSetOptions.FORCE;
import static marmot.plan.SpatialJoinOptions.WITHIN_DISTANCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.ExecutePlanOptions;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSpatialJoin {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "POI/주유소_가격";
//	private static final String INPUT = "주소/건물POI";
//	private static final String PARAM = "지오비전/집계구/2018";
//	private static final String PARAM = "구역/집계구";
	private static final String PARAM = "교통/지하철/서울역사";

	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Plan plan = Plan.builder("spatial_join")
//							.load(INPUT, LoadOptions.FIXED_MAPPERS)
							.load(INPUT)
							.spatialJoin("the_geom", PARAM, "the_geom,상호", WITHIN_DISTANCE(500))
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan, ExecutePlanOptions.DISABLE_LOCAL_EXEC);
		DataSet result = marmot.getDataSet(RESULT);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
		System.out.println("elapsed: " + watch.getElapsedSecondString());
	}
}
