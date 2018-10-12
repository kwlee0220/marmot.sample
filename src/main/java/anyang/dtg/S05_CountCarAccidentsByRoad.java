package anyang.dtg;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.plan.SpatialJoinOption.WITHIN_DISTANCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S05_CountCarAccidentsByRoad {
	private static final String ACCIDENT = "분석결과/안양대/도봉구/사망사고";
	private static final String ROADS = "기타/안양대/도봉구/도로망";
	private static final String OUTPUT = "분석결과/안양대/도봉구/도로별_사망사고_빈도";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		AggregateFunction[] aggrs = new AggregateFunction[]{ COUNT() };

		Plan plan;
		plan = marmot.planBuilder("도로별 사망사고 빈도집계")
					.load(ROADS)
					.spatialAggregateJoin("the_geom", ACCIDENT, aggrs, WITHIN_DISTANCE(15))
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(ACCIDENT).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
