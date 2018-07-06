package geom;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadHexagonGridFile {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	private static final double SIDE_LEN = 150;
	
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
		
		DataSet dataset = marmot.getDataSet(INPUT);
		String geomCol = dataset.getGeometryColumn();
		String srid = dataset.getGeometryColumnInfo().srid();
		Envelope bounds = dataset.getBounds();
		bounds.expandBy(2*SIDE_LEN, SIDE_LEN);

		Plan plan = marmot.planBuilder("load_hexagon_grid")
							.loadHexagonGridFile(bounds, srid, SIDE_LEN, 8)
							.spatialSemiJoin("the_geom", INPUT, INTERSECTS)
							.store(RESULT)
							.build();
		DataSet result = marmot.createDataSet(RESULT, dataset.getGeometryColumnInfo(), plan, true);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
