package misc;


import static marmot.ExecutePlanOption.DISABLE_LOCAL_EXEC;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.plan.LoadJdbcTableOption.MAPPER_COUNT;
import static marmot.plan.LoadJdbcTableOption.SELECT;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
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

		Plan plan = marmot.planBuilder("test")
							.loadJdbcTable(JDBC_URL, USER, PASSWD, DRIVER_CLASS, TABLE_NAME,
											SELECT("ST_AsBinary(the_geom) as the_geom"),
											MAPPER_COUNT(7))
							.aggregate(COUNT())
							.build();
		long count = marmot.executeToLong(plan, DISABLE_LOCAL_EXEC).get();
		System.out.println("count=" + count);
	}
}
