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
public class S01_FilterDoBongGuDTG {
	private static final String DOBONG_GU = "기타/안양대/도봉구/전체구역";
	private static final String DTG = "교통/dtg";
	private static final String OUTPUT = "분석결과/안양대/도봉구/DTG";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Geometry dobong = getDoBongGuRegion(marmot);

		Plan plan;
		plan = marmot.planBuilder("도봉구_영역_DTG 추출")
						.query(DTG, SpatialRelation.INTERSECTS, dobong)
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
