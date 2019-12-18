package bizarea;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.JoinOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1FlowPop {
	private static final String BIZ_GRID = "tmp/bizarea/grid100";
	private static final String FLOW_POP = "주민/유동인구/월별_시간대/2015";
	private static final String RESULT = "tmp/bizarea/grid100_pop";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		String handleNull = IntStream.range(0, 24)
				.mapToObj(idx -> String.format("if ( avg_%02dtmst == null ) { avg_%02dtmst = 0; }%n", idx, idx))
				.collect(Collectors.joining());

		String avgExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		avgExpr = String.format("(%s)/24", avgExpr);
		
		DataSet ds = marmot.getDataSet(BIZ_GRID);
		String geomCol = ds.getGeometryColumn();
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan = Plan.builder("대도시 상업지역 구역별 유동인구수 집계")
							.load(FLOW_POP)
							.update(handleNull)
							// 시간대 단위의 유동인구는 모두 합쳐 하루 매출액을 계산한다. 
							.defineColumn("flow_pop:double", avgExpr)
							.project("std_ym,block_cd,flow_pop")
							// BIZ_GRID와 소지역 코드를 이용하여 조인하여, 대도시 상업지역과 겹치는
							// 유동인구 구역을 뽑는다. 
							.hashJoin("block_cd", BIZ_GRID, "block_cd",
									"param.*,std_ym,flow_pop", JoinOptions.INNER_JOIN(32))
							// 한 그리드 셀에 여러 소지역 유동인구 정보가 존재하면,
							// 해당 유동인구들의 평균을 구한다.
							.aggregateByGroup(Group.ofKeys("std_ym,cell_id")
													.tags(geomCol + ",sgg_cd")
													.workerCount(3),
												AVG("flow_pop").as("flow_pop"))
							.project(String.format("%s,*-{%s}", geomCol, geomCol))
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
