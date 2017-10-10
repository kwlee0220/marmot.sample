package bizarea;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.optor.JoinOptions;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step2 {
	private static final String BIZ_GRID_SALES = "tmp/bizarea/grid100_sales";
	private static final String BIZ_GRID_FLOW_POP = "tmp/bizarea/grid100_pop";
	private static final String POLITICAL = "구역/통합법정동";
	private static final String RESULT = "tmp/bizarea/grid100_result";
	
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
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		DataSet info = marmot.getDataSet(BIZ_GRID_SALES);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		String script = "if ( std_ym == null ) {std_ym = param_std_ym;}"
						+ "if ( cell_id == null ) {cell_id = param_cell_id;}"
						+ "if ( sgg_cd == null ) {sgg_cd = param_sgg_cd;}";

		Plan plan = marmot.planBuilder("대도시 상업지역 구역별 카드매출액 및 유동인구수 통합")
								.load(BIZ_GRID_SALES)
								.join("std_ym,cell_id,sgg_cd", BIZ_GRID_FLOW_POP,
										"std_ym,cell_id,sgg_cd",
										"*, param.{"
											+ "the_geom as param_the_geom,"
											+ "std_ym as param_std_ym,"
											+ "cell_id as param_cell_id,"
											+ "sgg_cd as param_sgg_cd,"
											+ "flow_pop}", new JoinOptions().workerCount(16))
								.update(script)
								.project("*-{param_the_geom,param_std_ym,param_cell_id,param_sgg_cd}")
								// 최종 결과에 행정도 코드를 부여한다.
								.spatialJoin("the_geom", POLITICAL, INTERSECTS,
											"*-{cell_pos},param.*-{the_geom,sgg_cd}")
								.store(RESULT)
								.build();
		
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		DataSet result = marmot.createDataSet(RESULT, schema, geomCol, srid, true);
		marmot.execute(plan);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
