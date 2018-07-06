package demo.dtg;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TagDtgWithGridAndRoadLink {
	private static final String POLITICAL = "구역/시도";
	private static final String DTG = "교통/dtg_s";
	private static final String ROAD_BUFFERED = "tmp/dtg/road_buffered";
	private static final String RESULT = "tmp/dtg/taggeds";
	
	private static final Size2d CELL_SIZE = new Size2d(100, 100);
	
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
		
		Geometry sidoGeom = getGyoungGiDo(marmot);
		Envelope bounds = sidoGeom.getEnvelopeInternal();
		
		DataSet output;
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:5186");

		Plan plan;
		plan = marmot.planBuilder("tag grid_cell and road_link")
					.load(DTG)
					.filter("x좌표 != 0 && y좌표 != 0")
					.expand("the_geom:point", "the_geom = ST_Point(x좌표,y좌표)")
					.transformCRS("the_geom", "EPSG:4326", "the_geom", "EPSG:5186")
					.assignSquareGridCell("the_geom", bounds, CELL_SIZE)
					.centroid("cell_geom", "grid")
					.spatialJoin("the_geom", ROAD_BUFFERED, INTERSECTS,
								"*,param.link_id")
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
}
