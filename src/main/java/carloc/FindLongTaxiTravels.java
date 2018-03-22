package carloc;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
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
public class FindLongTaxiTravels {
	private static final String TAXI_TRJ = "로그/나비콜/택시경로";
	private static final String RESULT = "tmp/result";
	private static final String SRID = "EPSG:5186";
	
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

		Plan plan = marmot.planBuilder("find_long_travels")
								.load(TAXI_TRJ)
								.filter("status == 3")
								.expand("length:double",
										"length = ST_TRLength(trajectory)")
								.pickTopK("length:D", 10)
								.expand("the_geom:line_string",
										"the_geom = ST_TRLineString(trajectory)")
								.project("*-{trajectory}")
								.store(RESULT)
								.build();
		DataSet result = marmot.createDataSet(RESULT, new GeometryColumnInfo("the_geom", SRID),
												plan, true);
		
		SampleUtils.printPrefix(result, 5);
	}
}
