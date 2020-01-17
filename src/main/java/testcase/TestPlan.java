package testcase;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.Group;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestPlan {
	public static void main(String[] args) throws Exception {
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet("POI/서울_종합병원")
											.getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder()
					.load("나비콜/택시로그")
					.filter("status == 1 || status == 2")
					.spatialJoin("the_geom", "POI/서울_종합병원", "param.{the_geom,bplc_nm}",
								SpatialJoinOptions.WITHIN_DISTANCE(300))
					.aggregateByGroup(Group.ofKeys("bplc_nm").tags("the_geom"),
										AggregateFunction.COUNT())
					.sort("count:d")
					.store("tmp/result", StoreDataSetOptions.FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
	}
}
