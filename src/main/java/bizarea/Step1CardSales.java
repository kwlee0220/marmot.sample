package bizarea;

import static marmot.optor.AggregateFunction.SUM;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
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

		String sumExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		sumExpr = "daily_sales="+sumExpr;
		
		DataSet ds = marmot.getDataSet(BIZ_GRID);
		String geomCol = ds.getGeometryColumn();
		
		Plan plan = marmot.planBuilder("대도시 상업지역 구역별 카드 일매출 집계")
								// 전국 카드매출액 파일을 읽는다.
								.load(CARD_SALES)
								// 시간대 단위의 매출액은 모두 합쳐 하루 매출액을 계산한다. 
								.expand("daily_sales:double", sumExpr)
								.project("std_ym,block_cd,daily_sales")
								// BIZ_GRID와 소지역 코드를 이용하여 조인하여, 대도시 상업지역과 겹치는
								// 매출액 구역을 뽑는다.
								.join("block_cd", BIZ_GRID, "block_cd",
										"param.*,std_ym,daily_sales",
										new JoinOptions().workerCount(64))
								// 한 그리드 셀에 여러 소지역 매출액 정보가 존재하면,
								// 해당 매출액은 모두 더한다. 
								.groupBy("std_ym,cell_id")
									.taggedKeyColumns(geomCol + ",sgg_cd")
									.workerCount(3)
									.aggregate(SUM("daily_sales").as("daily_sales"))
								.project(String.format("%s,*-{%s}", geomCol, geomCol))
								.store(RESULT)
								.build();
		DataSet result = marmot.createDataSet(RESULT, ds.getGeometryColumnInfo(), plan, true);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
