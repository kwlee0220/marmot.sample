package demo.dtg;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.JoinOptions.SEMI_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.geo.command.CreateSpatialIndexOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TagDtgWithRoad {
	private static final String POLITICAL = "구역/시도";
	private static final String DTG = "교통/dtg";
	private static final String DTG_COMPANY = "교통/dtg_companies";
	private static final String ROAD = "교통/도로/링크";
	private static final String TEMP_ROAD = "tmp/dtg/road2";
	private static final String TEMP_CARGOS = "tmp/dtg/cargos";
	private static final String RESULT = "tmp/dtg/histogram_road";
	
	private static final double DIST = 15d;
	private static final int WORKER_COUNT = 5;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Polygon key = getValidWgsBounds(marmot);
		
		excludeHighway(marmot, TEMP_ROAD);
		filterCargo(marmot, TEMP_CARGOS);
		
		DataSet ds = marmot.getDataSet(DTG);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		int nworkers = (int)(ds.length() / UnitUtils.parseByteSize("20gb"));
		
		DataSet output;

		Plan plan;
		plan = Plan.builder("calc_histogram_road_links")
					.load(DTG)

					.toPoint("x좌표", "y좌표", "the_geom")
					.filterSpatially("the_geom", INTERSECTS, key)
					
					.project("the_geom,운송사코드")
					.hashJoin("운송사코드", TEMP_CARGOS, "회사코드", "the_geom", SEMI_JOIN(nworkers))
					
					.transformCrs("the_geom", "EPSG:4326", "EPSG:5186")
					.knnJoin("the_geom", TEMP_ROAD, 1, DIST, "param.{the_geom, link_id, road_name}")

					.aggregateByGroup(Group.ofKeys("link_id").tags("the_geom")
											.workerCount(WORKER_COUNT),
										COUNT())

					.store(RESULT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		output = marmot.getDataSet(RESULT);
		
		watch.stop();
		System.out.printf("count=%d, total elapsed time=%s%n",
							output.getRecordCount(), watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(output, 5);
	}
	
	private static Polygon getValidWgsBounds(PBMarmotClient marmot) {
		Envelope bounds = marmot.getDataSet(POLITICAL).getBounds();
		bounds.expandBy(1);
		
		CoordinateTransform trans = CoordinateTransform.get("EPSG:5186", "EPSG:4326");
		Envelope wgs84Bounds = trans.transform(bounds);
		
		return GeoClientUtils.toPolygon(wgs84Bounds);
	}
	
	private static void excludeHighway(PBMarmotClient marmot, String outDsId) {
		GeometryColumnInfo gcInfo = marmot.getDataSet(ROAD).getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder("exclude highway")
					.load(ROAD)
					.filter("road_rank != '101'")
					.filter("road_type == '000' || road_type == '003'")
					.filter("road_use == '0'")
					.project("the_geom,link_id,road_name")
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet output = marmot.getDataSet(outDsId);
		output.cluster(CreateSpatialIndexOptions.WORKER_COUNT(1));
	}
	
	private static DataSet filterCargo(PBMarmotClient marmot, String output) {
		Plan plan;
		plan = Plan.builder("find cargos")
					.load(DTG_COMPANY)
					.filter("업종코드 == 31 || 업종코드 == 32")
					.store(output, FORCE)
					.build();
		marmot.execute(plan);
		
		return marmot.getDataSet(output);
	}
}
