package anyang.dtg;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.geo.SpatialRelation.WITHIN_DISTANCE;
import static marmot.plan.SpatialJoinOption.JOIN_EXPR;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.plan.RecordScript;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S07_CountDtgByRoad {
	private static final String DTG = "분석결과/안양대/도봉구/DTG";
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
		
		RecordScript script = RecordScript.of("$pat = ST_DTPattern(\"yyyyMMddHHmmss\")",
											"ST_DTParseLE(운행일자 + 운행시분초.substring(0,6), $pat)");

		Plan plan;
		plan = marmot.planBuilder("읍면동별 DTG 빈도집계")
					.load(DTG)
					.filter("운행속도 > 0")
					.expand1("ts:datetime", script)
					.spatialJoin("the_geom", ROADS, "param.*,차량번호,ts",
								JOIN_EXPR(WITHIN_DISTANCE(15)))
					.groupBy("db_id,차량번호")
						.tagWith("the_geom,id")
						.run(aggrPlan)
					.groupBy("db_id")
						.tagWith("the_geom,id")
						.aggregate(AggregateFunction.SUM("count").as("count"))
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(ROADS).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
