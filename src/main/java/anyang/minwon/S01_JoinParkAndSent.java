package anyang.minwon;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
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
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(PARK).getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("이름기반 공원 감성분석 맵매칭")
					.load(PARK)
					.filter("!kor_par_nm.equals('#N/A')")
					.hashJoin("kor_par_nm", EMOTION, "poi", "the_geom,param.*-{the_geom}",
							JoinOptions.INNER_JOIN)
					.store(OUTPUT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.close();
	}
}
