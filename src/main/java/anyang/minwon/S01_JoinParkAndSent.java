package anyang.minwon;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClient;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S01_JoinParkAndSent {
	private static final String PARK = "기타/안양대/도봉구/공원";
	private static final String EMOTION = "기타/안양대/도봉구/공원_감성분석";
	private static final String OUTPUT = "분석결과/안양대/도봉구/공원_감석분석_맵";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();

		Plan plan;
		plan = marmot.planBuilder("이름기반 공원 감성분석 맵매칭")
					.load(PARK)
					.filter("!kor_par_nm.equals('#N/A')")
					.join("kor_par_nm", EMOTION, "poi", "the_geom,param.*-{the_geom}",
							JoinOptions.INNER_JOIN())
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(PARK).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
