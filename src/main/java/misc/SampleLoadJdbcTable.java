package misc;


import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import marmot.ExecutePlanOptions;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.plan.JdbcConnectOptions;
import marmot.plan.LoadJdbcTableOptions;
import marmot.remote.protobuf.PBMarmotClient;

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
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		JdbcConnectOptions jdbcOpts = JdbcConnectOptions.create()
														.jdbcUrl(JDBC_URL)
														.user(USER)
														.passwd(PASSWD)
														.driverClassName(DRIVER_CLASS);
		Plan plan = marmot.planBuilder("test")
							.loadJdbcTable(TABLE_NAME, jdbcOpts,
											LoadJdbcTableOptions.create()
												.selectExpr("ST_AsBinary(the_geom) as the_geom")
												.mapperCount(7))
							.aggregate(COUNT())
							.build();
		long count = marmot.executeToLong(plan, ExecutePlanOptions.create().disableLocalExecution(true)).get();
		System.out.println("count=" + count);
	}
}
