package demo.dtg;

import static marmot.optor.JoinOptions.INNER_JOIN;

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
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TagDtgFully {
	private static final String POLITICAL = "구역/시도";
	private static final String DTG = "교통/dtg_s";
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

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		marmot.setMapOutputCompression(true);
		
		Geometry sidoGeom = getGyoungGiDo(marmot);
		Envelope bounds = sidoGeom.getEnvelopeInternal();
		
		DataSet dtg = marmot.getDataSet(DTG);
		int joinWorkers = (int)(dtg.length() / UnitUtils.parseByteSize("20gb"));
		
		DataSet output;
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:5186");

		Plan plan;
		plan = marmot.planBuilder("tag dtg")
					.load(DTG)
					
					.update("x좌표 = (x좌표==0) ? null : x좌표;"
							+ "y좌표 = (y좌표==0) ? null : y좌표;")
					.filter("x좌표 == null")
					
//					.join("운송사코드", DTG_COMPANY, "회사코드", "*,param.업종코드",
//							INNER_JOIN(joinWorkers))

					.toPoint("x좌표", "y좌표", "the_geom")
					.transformCRS("the_geom", "EPSG:4326", "the_geom", "EPSG:5186")
					.knnJoin("the_geom", ROAD, DIST, 1, "*-{x좌표,y좌표},param.{link_id}")
//					
//					.assignSquareGridCell("the_geom", bounds, CELL_SIZE)
//					.centroid("cell_geom", "grid")
					
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
