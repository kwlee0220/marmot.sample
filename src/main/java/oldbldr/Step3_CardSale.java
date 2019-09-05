package oldbldr;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.JoinOptions.INNER_JOIN;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step3_CardSale {
	private static final String CARD_SALES = "주민/카드매출/월별_시간대/2015";
	private static final String BLOCKS = "tmp/oldbld/blocks_emd";
	private static final String RESULT = "tmp/oldbld/card_sale_emd";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		String sumExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		
		Plan plan;
		plan = marmot.planBuilder("읍면동별 2015년도 카드매출 집계")
					.load(CARD_SALES)
					.defineColumn("sale_amt:double", sumExpr)
					.project("block_cd,sale_amt")
					.aggregateByGroup(Group.ofKeys("block_cd"), SUM("sale_amt").as("sale_amt"))
					.hashJoin("block_cd", BLOCKS, "block_cd", "*,param.{emd_cd}", INNER_JOIN)
					.aggregateByGroup(Group.ofKeys("emd_cd").workerCount(1),
										SUM("sale_amt").as("sale_amt"))
					.store(RESULT, FORCE)
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
