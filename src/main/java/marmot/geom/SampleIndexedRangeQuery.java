package marmot.geom;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClient;
import marmot.geo.GeoClientUtils;
import marmot.remote.protobuf.PBMarmotClient;

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
		PBMarmotClient marmot = MarmotClient.connect();
		
		Envelope bounds = GeoClientUtils.expandBy(getBorder(marmot).getEnvelopeInternal(), -14000);
		Geometry key = GeoClientUtils.toPolygon(bounds);

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Plan plan = marmot.planBuilder("sample_indexed_rangequery")
						.query(BUILDINGS, INTERSECTS, key)
						.project("the_geom,시군구코드,건물명")
						.build();
		DataSet result = marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
	
	private static Geometry getBorder(MarmotRuntime marmot) {
		Plan plan = marmot.planBuilder("get seould")
							.load(SIDO)
							.filter("ctprvn_cd == 11")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get();
	}
}
