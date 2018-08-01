package anyang.energe;

import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

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
public class T05_2017ElectroGridAnalysis {
	private static final String INPUT = "tmp/anyang/cadastral_electro2017";
	private static final String OUTPUT = "tmp/anyang/grid_electro2017";
	
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
		
		DataSet cadastral = marmot.getDataSet(INPUT);
		Envelope bounds = cadastral.getBounds();
		Size2d cellSize = new Size2d(1000, 1000);
		
		Plan plan;
		plan = marmot.planBuilder("2017 전기 사용량 격자 분석")
					.load(INPUT)
					.assignSquareGridCell("the_geom", bounds, cellSize)
					.intersection("the_geom", "cell_geom", "overlap")
					.expand("ratio:double", "ratio = (ST_Area(overlap) /  ST_Area(the_geom))")
					.update("usage  *= ratio")
					.groupBy("cell_id")
						.tagWith("cell_geom,cell_pos")
						.workerCount(3)
						.aggregate(SUM("usage"))
					.expand("x:long,y:long", "x = cell_pos.getX(); y = cell_pos.getY()")
					.project("cell_geom as the_geom, x, y, *-{cell_geom,x,y}")
					.store(OUTPUT)
					.build();
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:5186");
		marmot.createDataSet(OUTPUT, info, plan, true);
		
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
	}
}
