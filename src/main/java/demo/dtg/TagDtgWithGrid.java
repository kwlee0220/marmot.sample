package demo.dtg;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.JoinOptions.SEMI_JOIN;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;
import utils.UnitUtils;

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
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		Geometry kyounggiGeom = getGyoungGiDo(marmot);
		Envelope bounds = kyounggiGeom.getEnvelopeInternal();
		Polygon key = getValidWgsBounds(bounds);
		
		filterCargo(marmot, TEMP_CARGOS);
		
		DataSet ds = marmot.getDataSet(DTG);
		int nworkers = (int)(ds.length() / UnitUtils.parseByteSize("20gb"));
		
		DataSet output;
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:5186");

		Plan plan;
		plan = marmot.planBuilder("calc_histogram_road_links")
					.load(DTG)

					.toPoint("x좌표", "y좌표", "the_geom")
					.intersects("the_geom", key)
					
					.project("the_geom,운송사코드")
					.join("운송사코드", TEMP_CARGOS, "회사코드", "the_geom", SEMI_JOIN(nworkers))
					
					.transformCrs("the_geom", "EPSG:4326", "EPSG:5186", "the_geom")
					.assignSquareGridCell("the_geom", bounds, CELL_SIZE)
					.centroid("cell_geom")
					.intersects("the_geom", kyounggiGeom)
					
					.groupBy("cell_id")
						.tagWith("the_geom,cell_pos")
						.workerCount(WORKER_COUNT)
						.aggregate(COUNT())
						
					.expand("grid_x:int,grid_y:int", "grid_x = cell_pos.x; grid_y = cell_pos.y")
					.project("the_geom,grid_x,grid_y,count")
					
					.store(RESULT)
					.build();
		output = marmot.createDataSet(RESULT, info, plan, true);
		
		watch.stop();
		System.out.printf("count=%d, total elapsed time=%s%n",
							output.getRecordCount(), watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(output, 5);
	}
	
	private static Geometry getGyoungGiDo(PBMarmotClient marmot) {
		Plan plan;
		plan = marmot.planBuilder("find gyounggi")
					.load(POLITICAL)
					.filter("ctprvn_cd == 41")
					.project("the_geom")
					.build();
		
		return marmot.executeLocally(plan).getFirst().map(r -> r.getGeometry(0)).get();
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
		plan = marmot.planBuilder("find cargos")
					.load(DTG_COMPANY)
					.filter("업종코드 == 31 || 업종코드 == 32")
					.store(output)
					.build();

		return marmot.createDataSet(output, plan, true);
	}
}
