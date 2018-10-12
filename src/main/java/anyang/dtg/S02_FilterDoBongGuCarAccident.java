package anyang.dtg;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S02_FilterDoBongGuCarAccident {
	private static final String DOBONG_GU = "기타/안양대/도봉구/전체구역";
	private static final String DEATH_ACCIDENT = "교통/교통사고/사망사고";
	private static final String OUTPUT = "분석결과/안양대/도봉구/사망사고";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Geometry dobong = getDoBongGuRegion(marmot);

		Plan plan;
		plan = marmot.planBuilder("도봉구_사망교통사고_추출")
						.query(DEATH_ACCIDENT, SpatialRelation.INTERSECTS, dobong)
						.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(DEATH_ACCIDENT).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
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
