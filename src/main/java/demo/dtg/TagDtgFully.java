package demo.dtg;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;

import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordScript;
import marmot.command.MarmotClientCommands;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SquareGrid;
import marmot.plan.GeomOpOption;
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
public class TagDtgFully {
	private static final String POLITICAL = "구역/시도";
	private static final String DTG = "교통/dtg";
	private static final String DTG_COMPANY = "교통/dtg_companies";
	private static final String ROAD = "교통/도로/링크";
	private static final String RESULT = "tmp/dtg/taggeds";
	
	private static final Size2d CELL_SIZE = new Size2d(100, 100);
	private static final double DIST = 15d;
	
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
		marmot.setMapOutputCompression(true);
		
//		Geometry sidoGeom = getGyoungGiDo(marmot);
//		Envelope bounds = sidoGeom.getEnvelopeInternal();
		Envelope bounds = marmot.getDataSet(POLITICAL).getBounds();
		
		CoordinateTransform trans = CoordinateTransform.get("EPSG:5186", "EPSG:4326");
		Envelope bounds2 = new Envelope(bounds);
		bounds2.expandBy(5);
		Envelope wgs84Bounds = trans.transform(bounds2);
		Map<String,Object> arguments = Maps.newHashMap();
		arguments.put("bounds", GeoClientUtils.toPolygon(wgs84Bounds));
		
		DataSet dtg = marmot.getDataSet(DTG);
		int joinWorkers = (int)(dtg.length() / UnitUtils.parseByteSize("20gb"));
		
		DataSet output;

		Plan plan;
		plan = marmot.planBuilder("tag dtg")
					.load(DTG)
					
					.join("운송사코드", DTG_COMPANY, "회사코드", "*,param.업종코드",
							LEFT_OUTER_JOIN(joinWorkers))

					.toPoint("x좌표", "y좌표", "the_geom")
					.update(RecordScript.of("if ( !bounds.contains(the_geom) ) { the_geom = null; }")
								.addArgumentAll(arguments))
					.transformCrs("the_geom", "EPSG:4326", "EPSG:5186")
					.knnOuterJoin("the_geom", ROAD, 1, DIST, "*-{x좌표,y좌표},param.{link_id}")
					
					.assignSquareGridCell("the_geom", new SquareGrid(bounds, CELL_SIZE), false)
					.centroid("cell_geom", GeomOpOption.OUTPUT("grid"))
					
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
	
	private static Geometry getGyoungGiDo(PBMarmotClient marmot) {
		Plan plan;
		plan = marmot.planBuilder("find gyounggi")
					.load(POLITICAL)
					.filter("ctprvn_cd == 41")
					.project("the_geom")
					.build();
		
		return marmot.executeToGeometry(plan).get();
	}
}
