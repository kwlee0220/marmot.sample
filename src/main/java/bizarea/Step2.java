package bizarea;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
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

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(BIZ_GRID_SALES);
		String geomCol = ds.getGeometryColumn();
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		String script = "if ( std_ym == null ) {std_ym = param_std_ym;}"
						+ "if ( cell_id == null ) {cell_id = param_cell_id;}"
						+ "if ( sgg_cd == null ) {sgg_cd = param_sgg_cd;}";

		Plan plan = Plan.builder("대도시 상업지역 구역별 카드매출액 및 유동인구수 통합")
							.load(BIZ_GRID_SALES)
							.hashJoin("std_ym,cell_id,sgg_cd", BIZ_GRID_FLOW_POP,
									"std_ym,cell_id,sgg_cd",
									"*, param.{"
										+ "the_geom as param_the_geom,"
										+ "std_ym as param_std_ym,"
										+ "cell_id as param_cell_id,"
										+ "sgg_cd as param_sgg_cd,"
										+ "flow_pop}", JoinOptions.INNER_JOIN(16))
							.update(script)
							.project("*-{param_the_geom,param_std_ym,param_cell_id,param_sgg_cd}")
							// 최종 결과에 행정도 코드를 부여한다.
							.spatialJoin("the_geom", POLITICAL,
										"*-{cell_pos},param.*-{the_geom,sgg_cd}")
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
