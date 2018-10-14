package anyang.dtg;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

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
public class B02_CountCarAccidentsByEMD {
	private static final String EMD = "기타/안양대/도봉구/행정동_구역";
	private static final String ACCIDENT = "분석결과/안양대/도봉구/사망사고";
	private static final String OUTPUT = "분석결과/안양대/도봉구/읍면동별_사망사고_빈도";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan;
		plan = marmot.planBuilder("읍면동별 사망사고 빈도집계")
						.load(EMD)
						.spatialAggregateJoin("the_geom", ACCIDENT, AggregateFunction.COUNT())
						.project("the_geom,db_id,행정동 as name,count")
						.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(ACCIDENT).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
