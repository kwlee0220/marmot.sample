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
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleIndexedRangeQuery {
	private static final String RESULT = "tmp/result";
	private static final String SIDO = "구역/시도";
	private static final String BUILDINGS = "주소/건물POI";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
//		Envelope bounds = GeoClientUtils.expandBy(getBorder(marmot).getEnvelopeInternal(), -14000);
		Envelope bounds = new Envelope(193189.76162216233, 202242.27243390775,
										550547.406706411, 552863.5428850177);
//		Geometry key = GeoClientUtils.toPolygon(bounds);

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Plan plan = Plan.builder("sample_indexed_rangequery")
							.query(BUILDINGS, bounds)
							.aggregate(AggregateFunction.COUNT())
//							.project("the_geom,시군구코드,건물명")
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
}
