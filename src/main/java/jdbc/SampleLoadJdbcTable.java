package jdbc;

import static marmot.plan.LoadJdbcTableOption.MAPPER_COUNT;
import static marmot.plan.LoadJdbcTableOption.SELECT;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.CreateDataSetParameters;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadJdbcTable {
	private static final String JDBC_URL = "jdbc:postgresql://129.254.82.95:5433/sbdata";
	private static final String USER = "sbdata";
	private static final String PASSWD = "urc2004";
	private static final String DRIVER_CLASS = "org.postgresql.Driver";
//	private static final String TABLE_NAME = "subway_stations";
//	private static final String TABLE_NAME = "cadastral";
	private static final String TABLE_NAME = "buildings";
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

		Plan plan = marmot.planBuilder("test")
							.loadJdbcTable(JDBC_URL, USER, PASSWD, DRIVER_CLASS, TABLE_NAME,
											SELECT("ST_AsBinary(the_geom) as the_geom"),
											MAPPER_COUNT(7))
//							.expand("the_geom:multi_polygon",
//									"the_geom = ST_GeomFromWKB(the_geom)")
							.aggregate(AggregateFunction.COUNT())
							.store(RESULT)
							.build();
		CreateDataSetParameters params = new CreateDataSetParameters(RESULT, plan, true)
																.setForce();
		DataSet result = marmot.createDataSet(params);
		watch.stop();
		
		SampleUtils.printPrefix(result, 1000);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
