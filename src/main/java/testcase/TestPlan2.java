package testcase;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.optor.StoreDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestPlan2 {
	public static void main(String[] args) throws Exception {
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet("POI/서울_종합병원")
											.getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder()
					.load("POI/서울_종합병원")
					.buffer("the_geom", 500)
					.spatialAggregateJoin("the_geom", "/교통/지하철/서울역사",
											AggregateFunction.COUNT())
					.project("the_geom,bplc_nm,count")
					.store("tmp/result", StoreDataSetOptions.FORCE)
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
	}
}
