package demo.dtg;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.JoinOptions.SEMI_JOIN;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
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
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		Polygon key = getValidWgsBounds(marmot);
		
		excludeHighway(marmot, TEMP_ROAD);
		filterCargo(marmot, TEMP_CARGOS);
		
		DataSet ds = marmot.getDataSet(DTG);
		int nworkers = (int)(ds.length() / UnitUtils.parseByteSize("20gb"));
		
		DataSet output;

		Plan plan;
		plan = marmot.planBuilder("calc_histogram_road_links")
					.load(DTG)

					.toPoint("x좌표", "y좌표", "the_geom")
					.filterSpatially("the_geom", INTERSECTS, key)
					
					.project("the_geom,운송사코드")
					.hashJoin("운송사코드", TEMP_CARGOS, "회사코드", "the_geom", SEMI_JOIN(nworkers))
					
					.transformCrs("the_geom", "EPSG:4326", "EPSG:5186")
					.knnJoin("the_geom", TEMP_ROAD, 1, DIST, "param.{the_geom, link_id, road_name}")
					
					.groupBy("link_id")
						.withTags("the_geom")
						.workerCount(WORKER_COUNT)
						.aggregate(COUNT())
					
					.store(RESULT)
					.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		output = marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		
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
		plan = marmot.planBuilder("exclude highway")
					.load(ROAD)
					.filter("road_rank != '101'")
					.filter("road_type == '000' || road_type == '003'")
					.filter("road_use == '0'")
					.project("the_geom,link_id,road_name")
					.store(outDsId)
					.build();
		DataSet output = marmot.createDataSet(outDsId, plan, GEOMETRY(gcInfo), FORCE);
		
		ClusterDataSetOptions opts = ClusterDataSetOptions.create().workerCount(1);
		output.cluster(opts);
	}
	
	private static DataSet filterCargo(PBMarmotClient marmot, String output) {
		Plan plan;
		plan = marmot.planBuilder("find cargos")
					.load(DTG_COMPANY)
					.filter("업종코드 == 31 || 업종코드 == 32")
					.store(output)
					.build();

		return marmot.createDataSet(output, plan, FORCE);
	}
}
