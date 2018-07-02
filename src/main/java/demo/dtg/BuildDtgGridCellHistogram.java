package demo.dtg;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import static marmot.optor.geo.SpatialRelation.*;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildDtgGridCellHistogram {
	private static final String POLITICAL = "구역/시도";
	private static final String DTG = "로그/나비콜/택시로그";
	private static final String ROAD = "교통/도로/링크";
	private static final String RESULT = "tmp/dtg/histogram";
	
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
		
		DataSet output;
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Envelope bounds = marmot.getDataSet(POLITICAL).getBounds();

		Plan plan = marmot.planBuilder("build_dtb_histogram")
							.load(DTG)
							.assignSquareGridCell("the_geom", bounds, CELL_SIZE)
							.groupBy("cell_id")
								.tagWith("cell_geom,cell_pos")
								.aggregate(COUNT().as("count"))
							.project("cell_geom as the_geom,cell_id,cell_pos,count")
							.centroid("the_geom", "the_geom")
							.spatialJoin("the_geom", ROAD, WITHIN_DISTANCE(25), "*,param.link_id")
							.build();
		output = marmot.createDataSet(RESULT, info, plan, true);
		
		watch.stop();
		System.out.printf("count=%d, total elapsed time=%s%n",
							output.getRecordCount(), watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(output, 5);
	}
}
