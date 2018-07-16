package demo.dtg;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import marmot.DataSet;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.geo.CoordinateTransform;
import marmot.geo.GeoClientUtils;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CountInvalidGeoms {
	private static final String POLITICAL = "구역/시도";
	private static final String DTG = "교통/dtg";
	private static final String RESULT = "tmp/dtg/taggeds";
	
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
		marmot.setMapOutputCompression(false);
		
		Polygon bounds = getValidWgsBounds(marmot);

		Plan plan;
		plan = marmot.planBuilder("count invalid geometry records")
					.load(DTG)
					.toPoint("x좌표", "y좌표", "the_geom")
					.intersects("the_geom", bounds, true)
					.aggregate(COUNT())
					.store(RESULT)
					.build();
		DataSet output = marmot.createDataSet(RESULT, plan, true);
		try ( RecordSet rset = output.read() ) {
			long count = rset.nextCopy().get().getLong(0);
			
			watch.stop();
			System.out.printf("count=%d, total elapsed time=%s%n",
								count, watch.getElapsedMillisString());
		}
	}
	
	private static Polygon getValidWgsBounds(PBMarmotClient marmot) {
		Envelope bounds = marmot.getDataSet(POLITICAL).getBounds();
		bounds.expandBy(1);
		
		CoordinateTransform trans = CoordinateTransform.get("EPSG:5186", "EPSG:4326");
		Envelope wgs84Bounds = trans.transform(bounds);
		
		return GeoClientUtils.toPolygon(wgs84Bounds);
	}
}