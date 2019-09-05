package bizarea;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.SUM;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.JoinOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1CardSales {
	private static final String BIZ_GRID = "tmp/bizarea/grid100";
	private static final String CARD_SALES = "주민/카드매출/월별_시간대/2015";
	private static final String RESULT = "tmp/bizarea/grid100_sales";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		String sumExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		
		DataSet ds = marmot.getDataSet(BIZ_GRID);
		String geomCol = ds.getGeometryColumn();
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("대도시 상업지역 구역별 카드 일매출 집계")
							// 전국 카드매출액 파일을 읽는다.
							.load(CARD_SALES)
							// 시간대 단위의 매출액은 모두 합쳐 하루 매출액을 계산한다. 
							.defineColumn("daily_sales:double", sumExpr)
							.project("std_ym,block_cd,daily_sales")
							// BIZ_GRID와 소지역 코드를 이용하여 조인하여, 대도시 상업지역과 겹치는
							// 매출액 구역을 뽑는다.
							.hashJoin("block_cd", BIZ_GRID, "block_cd",
									"param.*,std_ym,daily_sales",
									JoinOptions.INNER_JOIN(64))
							// 한 그리드 셀에 여러 소지역 매출액 정보가 존재하면,
							// 해당 매출액은 모두 더한다. 
							.aggregateByGroup(Group.ofKeys("std_ym,cell_id")
													.tags(geomCol + ",sgg_cd")
													.workerCount(3),
												SUM("daily_sales").as("daily_sales"))
							.project(String.format("%s,*-{%s}", geomCol, geomCol))
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
