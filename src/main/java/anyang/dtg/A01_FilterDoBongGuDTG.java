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
public class A01_FilterDoBongGuDTG {
	private static final String DOBONG_GU = "기타/안양대/도봉구/전체구역";
	private static final String DTG = "교통/dtg";
	private static final String OUTPUT = "분석결과/안양대/도봉구/DTG";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Geometry dobong = getDoBongGuRegion(marmot);
//		String prjExpr = "the_geom,운행일자 as date, 운송사코드 as company,"
//						+ "차량번호 as car_no, 운행시분초 as time, 일일주행거리 as day_mile,"
//						+ "누적주행거리 as accum_mile, 운행속도 as speed, rpm,"
//						+ "브레이크신호 as brake, x좌표 as x_coord, y좌표 as y_coord,"
//						+ "방위각 as heading, 가속도x as acc_x, 가속도y as acc_y";
		
		Plan plan;
		plan = marmot.planBuilder("도봉구_영역_DTG 추출")
						.query(DTG, SpatialRelation.INTERSECTS, dobong)
//						.project(prjExpr)
						.transformCrs("the_geom", "EPSG:4326", "EPSG:5186")
						.shard(7)
						.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(DOBONG_GU).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
	
	private static Geometry getDoBongGuRegion(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("도봉구 영역 추출")
						.load(DOBONG_GU)
						.filter("sig_kor_nm == '도봉구'")
						.project("the_geom")
						.transformCrs("the_geom", "EPSG:5186", "EPSG:4326")
						.build();
		
		return marmot.executeToGeometry(plan).get();
	}
}
