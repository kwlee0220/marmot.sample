package misc;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.geo.SquareGrid;
import marmot.plan.GeomOpOption;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Test2017_1 {
	private static final String ADDR_BLD = "건물/위치";
	private static final String ADDR_BLD_UTILS = "tmp/test2017/buildings_utils";
	private static final String GRID = "tmp/test2017/grid30";
	
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

		DataSet input = marmot.getDataSet(ADDR_BLD);
		String srid = input.getGeometryColumnInfo().srid();
		Envelope bounds = input.getBounds();
		Size2d cellSize = new Size2d(30, 30);
		
		Plan plan = marmot.planBuilder("get_biz_grid")
								.loadSquareGridFile(new SquareGrid(ADDR_BLD, cellSize), -1)
								.centroid("the_geom")
//								.aggregateJoin("the_geom", ADDR_BLD_UTILS_CLTS,
//										SpatialRelation.WITHIN_DISTANCE(2000), COUNT())
								.buffer("the_geom", 100, GeomOpOption.OUTPUT("center"))
								.spatialAggregateJoin("center", ADDR_BLD_UTILS, COUNT())
								.project("the_geom,cell_id,count")
								.store(GRID)
								.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", srid);
		DataSet result = marmot.createDataSet(GRID, gcInfo, plan, DataSetOption.FORCE);
		watch.stop();
		
		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
