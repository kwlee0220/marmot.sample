package anyang.dtg;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.locationtech.jts.geom.Geometry;

import utils.StopWatch;

import common.SampleUtils;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class A01_FilterDoBongGuDTG {
	private static final String DOBONG_GU = "기타/안양대/도봉구/전체구역";
	private static final String DTG = "교통/dtg";
	private static final String OUTPUT = "분석결과/안양대/도봉구/DTG";
	
	public static final void main(String... args) throws Exception {
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Geometry dobong = getDoBongGuRegion(marmot);
//		String prjExpr = "the_geom,운행일자 as date, 운송사코드 as company,"
//						+ "차량번호 as car_no, 운행시분초 as time, 일일주행거리 as day_mile,"
//						+ "누적주행거리 as accum_mile, 운행속도 as speed, rpm,"
//						+ "브레이크신호 as brake, x좌표 as x_coord, y좌표 as y_coord,"
//						+ "방위각 as heading, 가속도x as acc_x, 가속도y as acc_y";

		GeometryColumnInfo gcInfo = marmot.getDataSet(DOBONG_GU).getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder("도봉구_영역_DTG 추출")
					.query(DTG, dobong.getEnvelopeInternal())
					.filterSpatially(gcInfo.name(), SpatialRelation.INTERSECTS, dobong)
//					.project(prjExpr)
					.transformCrs("the_geom", "EPSG:4326", "EPSG:5186")
					.shard(7)
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
		plan = Plan.builder("도봉구 영역 추출")
						.load(DOBONG_GU)
						.filter("sig_kor_nm == '도봉구'")
						.project("the_geom")
						.transformCrs("the_geom", "EPSG:5186", "EPSG:4326")
						.build();
		
		return marmot.executeToGeometry(plan).get();
	}
}
