package anyang.minwon;

import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S02_JoinParkAndSent_ID {
	private static final String PARK = "기타/안양대/도봉구/공원";
	private static final String EMOTION = "기타/안양대/도봉구/공원_감성분석";
	private static final String OUTPUT = "분석결과/안양대/도봉구/공원_감석분석_맵_ID";
	
	public static final void main(String... args) throws Exception {
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = marmot.getDataSet(PARK).getGeometryColumnInfo();
		String prjExpr = "the_geom,row_id,poi,id,sp,sn,언급빈도수 as mention,"
						+ "선호도 as preference";

		Plan plan;
		plan = Plan.builder("이름기반 공원 감성분석 맵매칭_ID")
					.load(PARK)
					.hashJoin("id", EMOTION, "id", "the_geom,param.*-{the_geom}",
							JoinOptions.INNER_JOIN)
					.project(prjExpr)
					.store(OUTPUT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.close();
	}
}
