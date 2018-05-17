package anyang.energe.gas;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import static marmot.optor.AggregateFunction.*;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S03_ElectroFishnetAnalysis {
	private static final String CADASTRAL = "구역/연속지적도_2017";
	private static final String CADASTRAL_ELEC_YEAR = "tmp/anyang/cadastral_electro";
	private static final String OUTPUT = "tmp/anyang/grid_electro";
	
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
		
		DataSet cadastral = marmot.getDataSet(CADASTRAL);
		Envelope bounds = cadastral.getBounds();
		Size2d cellSize = new Size2d(1000, 1000);

		DataSet ds = marmot.getDataSet(CADASTRAL_ELEC_YEAR);

		Plan plan;
		plan = marmot.planBuilder("전기 사용량 격자 분석")
					.load(CADASTRAL_ELEC_YEAR)
					.assignSquareGridCell("the_geom", bounds, cellSize)
					.intersection("the_geom", "cell_geom", "overlap")
					.expand("portion:double", "portion = ST_Area(overlap) /  ST_Area(the_geom)")
					.update("usage = usage * portion")
					.groupBy("cell_id")
						.taggedKeyColumns("cell_geom,cell_pos")
						.aggregate(SUM("usage").as("usage"))
					.expand("x:long,y:long", "x = cell_pos.getX(); y = cell_pos.getY()")
					.project("cell_geom as the_geom, x, y, usage")
					.store(OUTPUT)
					.build();
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:5186");
		DataSet result = marmot.createDataSet(OUTPUT, info, plan, true);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
//		result.cluster();
		SampleUtils.printPrefix(result, 20);
		
		marmot.disconnect();
	}
}
