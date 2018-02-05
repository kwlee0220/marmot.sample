package carloc;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.geo.GeoClientUtils;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcHeatMap {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String SEOUL = "시연/서울특별시";
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
		
		DataSet border = marmot.getDataSet(SEOUL);
		String srid = border.getSRID();
		Envelope envl = border.getBounds();
		Polygon key = GeoClientUtils.toPolygon(envl);
		
		Size2d cellSize = new Size2d(envl.getWidth() / 50,
														envl.getHeight() / 50);
		
		Plan plan = marmot.planBuilder("calc_heat_map")
							.loadSquareGridFile(envl, cellSize, 32)
							.spatialJoin("the_geom", TAXI_LOG, INTERSECTS, "*")
							.groupBy("cell_id")
								.taggedKeyColumns("the_geom")
								.aggregate(COUNT())
//							.aggregateJoin("the_geom", TAXI_LOG, INTERSECTS, COUNT())
							.store(RESULT)
							.build();
		DataSet result = marmot.createDataSet(RESULT, "the_geom", srid, plan, true);
		
		SampleUtils.printPrefix(result, 5);
	}
}
