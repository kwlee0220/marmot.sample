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
public class T03_EstimateIDWGas {
	private static final String CADASTRAL = "구역/연속지적도_2017";
	private static final String CADASTRAL_GAS_CENTER = "tmp/anyang/gas_cadastral_centers";
	private static final String OUTPUT = "tmp/anyang/idw_gas";
	
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
		
		Size2d cellSize = new Size2d(1000, 1000);
		DataSet ds = marmot.getDataSet(CADASTRAL_GAS_CENTER);

		Plan plan;
		plan = marmot.planBuilder("가스 사용량 격자 분석")
					.loadSquareGridFile(CADASTRAL, cellSize, 23)
					.estimateIDW("the_geom", CADASTRAL_GAS_CENTER, "usage", 15000d, -1, -1, "usage")
					.expand("x:long,y:long", "x = cell_pos.getX(); y = cell_pos.getY()")
					.project("the_geom, x, y, usage")
					.filter("usage > 0")
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