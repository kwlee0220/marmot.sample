package anyang.dtg;

import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B02_CountCarAccidentsByEMD {
	private static final String EMD = "기타/안양대/도봉구/행정동_구역";
	private static final String ACCIDENT = "분석결과/안양대/도봉구/사망사고";
	private static final String OUTPUT = "분석결과/안양대/도봉구/읍면동별_사망사고_빈도";
	
	public static final void main(String... args) throws Exception {
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(ACCIDENT).getGeometryColumnInfo();

		Plan plan;
		plan = Plan.builder("읍면동별 사망사고 빈도집계")
						.load(EMD)
						.spatialAggregateJoin("the_geom", ACCIDENT, AggregateFunction.COUNT())
						.project("the_geom,db_id,행정동 as name,count")
						.store(OUTPUT, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.close();
	}
}
