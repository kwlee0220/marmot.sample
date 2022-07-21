package anyang.dtg;

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
public class A02_CountDtgByEMD {
	private static final String DTG = "기타/안양대/도봉구/DTG";
	private static final String EMD = "기타/안양대/도봉구/행정동_구역";
	private static final String OUTPUT = "분석결과/안양대/도봉구/읍면동별_DTG_빈도";
	
	public static final void main(String... args) throws Exception {
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = marmot.getDataSet(EMD).getGeometryColumnInfo();
		Plan aggrPlan = Plan.builder("aggregate")
								.clusterChronicles("ts", "interval", "10m")
								.aggregate(AggregateFunction.COUNT())
								.build();

		Plan plan;
		plan = Plan.builder("읍면동별 DTG 빈도집계")
					.load(DTG)
					.filter("운행속도 > 0")
					.spatialJoin("the_geom", EMD,
								"param.*-{행정동},param.행정동 as hdong,차량번호 as car_no,ts")
					.runPlanByGroup(Group.ofKeys("db_id,car_no")
										.tags("the_geom,hdong"), aggrPlan)
					.aggregateByGroup(Group.ofKeys("db_id").tags("the_geom,hdong"),
									AggregateFunction.SUM("count").as("count"))
					.store(OUTPUT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.close();
	}
}
