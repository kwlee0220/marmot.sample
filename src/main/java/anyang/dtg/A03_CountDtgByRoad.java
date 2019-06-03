package anyang.dtg;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class A03_CountDtgByRoad {
	private static final String DTG = "기타/안양대/도봉구/DTG";
	private static final String ROADS = "기타/안양대/도봉구/도로망";
	private static final String OUTPUT = "분석결과/안양대/도봉구/도로별_DTG_빈도";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan aggrPlan = marmot.planBuilder("aggregate")
								.clusterChronicles("ts", "interval", "10m")
								.aggregate(AggregateFunction.COUNT())
								.build();

		Plan plan;
		plan = marmot.planBuilder("읍면동별 DTG 빈도집계")
					.load(DTG)
					.filter("운행속도 > 0")
					.spatialJoin("the_geom", ROADS, "param.*,차량번호,ts",
								SpatialJoinOptions.create().withinDistance(15))
					.runPlanByGroup(Group.ofKeys("db_id,차량번호")
										.tags("the_geom,id"), aggrPlan)
					.aggregateByGroup(Group.ofKeys("db_id").tags("the_geom,id"),
							AggregateFunction.SUM("count").as("count"))
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(ROADS).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
