package anyang.dtg;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class D03_CountCarAccidentsByRoad {
	private static final String ROADS = "교통/도로/링크";
	private static final String ACCIDENT = "교통/교통사고/사망사고";
	private static final String OUTPUT = "분석결과/안양대/네트워크/도로별_사망사고_분포";
	
	public static final void main(String... args) throws Exception {
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = marmot.getDataSet(ACCIDENT).getGeometryColumnInfo();
		AggregateFunction[] aggrs = new AggregateFunction[]{ COUNT() };

		Plan plan;
		plan = Plan.builder("도로별 사망사고 빈도집계")
					.load(ROADS)
					.spatialAggregateJoin("the_geom", ACCIDENT, aggrs,
										SpatialJoinOptions.WITHIN_DISTANCE(15))
					.project("the_geom,link_id,road_name,count")
					.store(OUTPUT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.close();
	}
}
