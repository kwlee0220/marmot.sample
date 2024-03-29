package marmot.geom;

import static marmot.optor.AggregateFunction.CONVEX_HULL;
import static marmot.optor.StoreDataSetOptions.FORCE;

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
public class SampleConvexHull {
	private static final String INPUT = "교통/버스/서울/정류소";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Plan plan = Plan.builder("convex_hull")
								.load(INPUT)
								.aggregate(CONVEX_HULL("the_geom").as("the_geom"))
								.store(RESULT, FORCE(gcInfo))
								.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
