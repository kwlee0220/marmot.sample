package geom;

import static marmot.DataSetOption.FORCE;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SquareGrid;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAssignSquareGridCell {
	private static final String INPUT = "POI/주유소_가격";
	private static final String BORDER = "시연/서울특별시";
	private static final String RESULT = "tmp/result";
	
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
		
		DataSet border = marmot.getDataSet(BORDER);
		Envelope envl = border.getBounds();
		Size2d cellSize = new Size2d(envl.getWidth() / 100,
														envl.getHeight() / 100);
		
		Plan plan = marmot.planBuilder("assign_fishnet_gridcell")
								.load(INPUT)
								.assignSquareGridCell("the_geom", new SquareGrid(envl, cellSize))
								.expand1("count:int", "1")
								.groupBy("cell_id")
									.tagWith("cell_geom,cell_pos")
									.workerCount(11)
									.aggregate(AggregateFunction.SUM("count").as("count"))
								.expand("x:int,y:int", "x = cell_pos.x; y = cell_pos.y")
								.project("cell_geom as the_geom,x,y,count")
								.store(RESULT)
								.build();
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE);
		watch.stop();

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
