package demo.dtg;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.JoinOptions.SEMI_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import utils.Size2d;
import utils.StopWatch;
import utils.UnitUtils;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TagDtgWithGrid {
	private static final String POLITICAL = "구역/시도";
	private static final String DTG = "교통/dtg";
	private static final String DTG_COMPANY = "교통/dtg_companies";
	private static final String TEMP_CARGOS = "tmp/dtg/cargos";
	private static final String RESULT = "tmp/dtg/histogram_grid";
	
	private static final Size2d CELL_SIZE = new Size2d(100, 100);
	private static final int WORKER_COUNT = 5;
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Geometry kyounggiGeom = getGyoungGiDo(marmot);
		Envelope bounds = kyounggiGeom.getEnvelopeInternal();
		Polygon key = getValidWgsBounds(bounds);
		
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
					.assignGridCell("the_geom", new SquareGrid(bounds, CELL_SIZE), false)
					.centroid("cell_geom")
					.filterSpatially("the_geom", INTERSECTS, kyounggiGeom)

					.aggregateByGroup(Group.ofKeys("cell_id").tags("the_geom,cell_pos")
											.workerCount(WORKER_COUNT),
										COUNT())
						
					.expand("grid_x:int,grid_y:int", "grid_x = cell_pos.x; grid_y = cell_pos.y")
					.project("the_geom,grid_x,grid_y,count")

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
	
	private static Geometry getGyoungGiDo(PBMarmotClient marmot) {
		Plan plan;
		plan = Plan.builder("find gyounggi")
					.load(POLITICAL)
					.filter("ctprvn_cd == 41")
					.project("the_geom")
					.build();
		
		return marmot.executeToGeometry(plan).get();
	}
	
	private static Polygon getValidWgsBounds(Envelope bounds) {
		Envelope bounds2 = new Envelope(bounds);
		bounds2.expandBy(1);
		
		CoordinateTransform trans = CoordinateTransform.get("EPSG:5186", "EPSG:4326");
		Envelope wgs84Bounds = trans.transform(bounds2);
		
		return GeoClientUtils.toPolygon(wgs84Bounds);
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
