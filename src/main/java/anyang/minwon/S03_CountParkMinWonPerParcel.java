package anyang.minwon;

import static marmot.optor.JoinOptions.RIGHT_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S03_CountParkMinWonPerParcel {
	private static final String PARK_MINWON = "기타/안양대/도봉구/공원_민원";
	private static final String PARCEL = "기타/안양대/도봉구/필지";
	private static final String OUTPUT = "분석결과/안양대/도봉구/필지별_공원민원수";
	
	public static final void main(String... args) throws Exception {
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = marmot.getDataSet(PARCEL).getGeometryColumnInfo();
		
		Plan plan;		
		plan = Plan.builder("필지별 공원관련 민원수 합계")
					.load(PARK_MINWON)
					.hashJoin("all_parcel_layer_id", PARCEL, "id", 
							"param.{the_geom,id},team_name", RIGHT_OUTER_JOIN)
					.aggregateByGroup(Group.ofKeys("id").tags("the_geom"),
										AggregateFunction.COUNT())
					.store(OUTPUT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.close();
	}
}
