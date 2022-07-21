package marmot.geom;

import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.ExecutePlanOptions;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadSpatialIndexJoin {
	private static final String RESULT = "tmp/result";
	private static final String OUTER = "교통/지하철/서울역사";
	private static final String INNER = "구역/시군구";

	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Plan plan = Plan.builder("load_spatial_index_join")
								.loadSpatialIndexJoin(OUTER, INNER,
													"left.*,right.{the_geom as the_geom2}")
								.intersection("the_geom", "the_geom2", "the_geom")
								.project("*-{the_geom2}")
								.store(RESULT, FORCE)
								.build();
		marmot.execute(plan, ExecutePlanOptions.DISABLE_LOCAL_EXEC);
		DataSet result = marmot.getDataSet(RESULT);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
	}
}
