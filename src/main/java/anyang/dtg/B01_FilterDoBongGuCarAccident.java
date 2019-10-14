package anyang.dtg;

import static marmot.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B01_FilterDoBongGuCarAccident {
	private static final String DOBONG_GU = "기타/안양대/도봉구/전체구역";
	private static final String DEATH_ACCIDENT = "교통/교통사고/사망사고";
	private static final String OUTPUT = "분석결과/안양대/도봉구/사망사고";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Geometry dobong = getDoBongGuRegion(marmot);
		String prjExpr = "the_geom,발생년 as year, 발생년월일시 as date_hour,"
						+ "발생분 as minute, 주야 as day_night, 요일 as week,"
						+ "사망자수 as death, 사상자수 as casualties, 중상자수 as heavy_inj,"
						+ "경상자수 as light_inj, 부상신고자수 as injury,발생지시도 as sido,"
						+ "발생지시군구 as sgg, 사고유형_대분류 as acc_major,"
						+ "사고유형_중분류 as acc_middle, 사고유형 as acc_type,"
						+ "법규위반_대분류 as vio_major, 법규위반 as violation,"
						+ "도로형태_대분류 as road_major, 도로형태 as road_type,"
						+ "당사자종별_1당_대분류 as type_1_major, 당사자종별_1당 as type_1,"
						+ "당사자종별_2당_대분류 as type_2_major, 당사자종별_2당 as type_2";
		GeometryColumnInfo gcInfo = marmot.getDataSet(DEATH_ACCIDENT).getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("도봉구_사망교통사고_추출")
						.query(DEATH_ACCIDENT, dobong)
						.project(prjExpr)
						.store(OUTPUT, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.close();
	}
	
	private static Geometry getDoBongGuRegion(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("get_dobong_gu")
						.load(DOBONG_GU)
						.filter("sig_kor_nm == '도봉구'")
						.project("the_geom")
						.build();
		return marmot.executeToGeometry(plan).get();
	}
}
