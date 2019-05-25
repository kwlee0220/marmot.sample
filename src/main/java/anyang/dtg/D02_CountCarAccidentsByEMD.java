package anyang.dtg;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class D02_CountCarAccidentsByEMD {
	private static final String EMD = "구역/읍면동";
	private static final String ACCIDENT = "교통/교통사고/사망사고";
	private static final String OUTPUT = "분석결과/안양대/네트워크/읍면동별_사망사고_분포";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan;
		plan = marmot.planBuilder("읍면동별_사망사고_분포")
						.load(EMD)
						.spatialAggregateJoin("the_geom", ACCIDENT, AggregateFunction.COUNT())
						.project("the_geom,emd_cd,emd_kor_nm,count")
						.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(ACCIDENT).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
