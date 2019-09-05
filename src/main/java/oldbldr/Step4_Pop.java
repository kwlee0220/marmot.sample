package oldbldr;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.AVG;
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
public class Step4_Pop {
	private static final String FLOW_POP = "주민/유동인구/월별_시간대/2015";
	private static final String BLOCKS = "tmp/oldbld/blocks_emd";
	private static final String RESULT = "tmp/oldbld/pop_emd";
	
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

		String avgExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.collect(Collectors.joining("+", "(", ")"));
		avgExpr = String.format("avg = %s / 24;", avgExpr);
		
		Plan plan;
		plan = marmot.planBuilder("읍면동별 2015년도 유동인구 집계")
					.load(FLOW_POP)
					.defineColumn("avg:double", avgExpr)
					.project("block_cd,avg")
					.aggregateByGroup(Group.ofKeys("block_cd"), AVG("avg").as("avg"))
					.hashJoin("block_cd", BLOCKS, "block_cd", "*,param.{emd_cd}", INNER_JOIN)
					.aggregateByGroup(Group.ofKeys("emd_cd").workerCount(1), AVG("avg").as("pop_avg"))
					.store(RESULT, FORCE)
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
