package misc;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.apache.log4j.PropertyConfigurator;

import io.vavr.control.Try;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.plan.JdbcConnectOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleStoreIntoJdbcTable {
	private static final String TABLE_NAME = "test";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		JdbcProcessor jdbc = new JdbcProcessor("jdbc:postgresql://129.254.82.95:5433/sbdata",
												"sbdata", "urc2004", "org.postgresql.Driver");
		
		DatabaseMetaData meta = jdbc.connect().getMetaData();
		
		Try.run(()->jdbc.executeUpdate("drop table " + TABLE_NAME));
		
		StringBuilder builder = new StringBuilder();
		builder.append(String.format("create table %s (", TABLE_NAME));
		builder.append("the_geom geometry(MultiPolygon), ");
		builder.append("sig_cd varchar");
		builder.append(")");
		jdbc.executeUpdate(builder.toString());
		
		JdbcConnectOptions jdbcOpts = JdbcConnectOptions.create()
											.jdbcUrl("jdbc:postgresql://129.254.82.95:5433/sbdata")
											.user("sbdata")
											.passwd("urc2004")
											.driverClassName("org.postgresql.Driver");
		Plan plan = marmot.planBuilder("test")
							.load("교통/지하철/서울역사")
							.project("the_geom,sig_cd")
							.expand("the_geom:binary", "the_geom = ST_AsBinary(the_geom)")
							.storeIntoJdbcTable(TABLE_NAME, jdbcOpts,
											"(the_geom,sig_cd) values (ST_GeomFromWKB(?), ?)")
							.build();
		marmot.execute(plan);
		
		try ( ResultSet rs = jdbc.executeQuery(String.format("select count(*) from %s", TABLE_NAME)) ) {
			rs.next();
			long count = rs.getLong(1);
			System.out.println("count = " + count);
		}
		
		Try.run(()->jdbc.executeUpdate("drop table " + TABLE_NAME));
	}
}
