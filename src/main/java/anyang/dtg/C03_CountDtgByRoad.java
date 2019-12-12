package anyang.dtg;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.RecordScript;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.optor.JoinOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class C03_CountDtgByRoad {
	private static final String DTG = "교통/dtg";
	private static final String ROADS = "교통/도로/링크";
	private static final String OUTPUT = "분석결과/안양대/네트워크/전국_도로별_통행량";
	private static final double RADIUS = 10;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		RecordScript script = RecordScript.of("$pat = ST_DTPattern(\"yyyyMMddHHmmss\")",
										"ST_DTParseLE(운행일자 + 운행시분초.substring(0,6), $pat)");
		Plan aggrPlan = marmot.planBuilder("aggregate")
								.clusterChronicles("ts", "interval", "10m")
								.aggregate(AggregateFunction.COUNT())
								.build();
		GeometryColumnInfo dtgInfo = marmot.getDataSet(DTG).getGeometryColumnInfo();
		GeometryColumnInfo gcInfo = marmot.getDataSet(ROADS).getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("전국_도로별_통행량")
					.load(DTG)
					.filter("운행속도 > 0")
					.defineColumn("ts:datetime", script)
					.transformCrs(dtgInfo.name(), dtgInfo.srid(), gcInfo.srid())
					.knnJoin("the_geom", ROADS, 1, RADIUS,
							"param.{link_id},차량번호 as car_no,ts")
					.runPlanByGroup(Group.ofKeys("link_id,car_no")
										.workerCount(37), aggrPlan)
					.aggregateByGroup(Group.ofKeys("link_id"),
									AggregateFunction.SUM("count").as("count"))
					.hashJoin("link_id", ROADS, "link_id", "param.{the_geom,link_id,road_name},count",
							JoinOptions.INNER_JOIN(1))
					.store(OUTPUT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.close();
	}
}
