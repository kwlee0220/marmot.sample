package marmot.geom;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleRangeQuery {
	private static final String RESULT = "tmp/result";
	private static final String SIDO = "구역/시도";
	private static final String EMD = "구역/읍면동";
//	private static final String BUILDINGS = "주소/건물POI";
//	private static final String INPUT = "주소/건물POI_clustered";
	private static final String INPUT = "교통/dtg_201609_clustered";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		Envelope bounds = getEMD(marmot, "가정동", gcInfo.srid());

		Plan plan = Plan.builder("sample_rangequery")
						.query(INPUT, bounds)
						.aggregate(AggregateFunction.COUNT())
//						.project("the_geom,시군구코드,건물명")
						.store(RESULT, FORCE)
						.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static Geometry getBorder(MarmotRuntime marmot) {
		Plan plan = Plan.builder("get seould")
							.load(SIDO)
							.filter("ctprvn_cd == 11")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get();
	}
	
	private static Envelope getEMD(MarmotRuntime marmot, String name, String targetSrid) {
		String expr = String.format("emd_kor_nm == '%s'", name);
		Plan plan = Plan.builder("get dong")
							.load(EMD)
							.filter(expr)
							.project("the_geom")
							.build();
		Envelope bounds = marmot.executeToGeometry(plan).get().getEnvelopeInternal();

		GeometryColumnInfo gcInfo = marmot.getDataSet(EMD).getGeometryColumnInfo();
		if ( gcInfo.srid() != targetSrid ) {
			bounds = CoordinateTransform.get(gcInfo.srid(), targetSrid)
										.transform(bounds);
		}
		
		return bounds;
	}
}
