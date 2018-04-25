package basic;

import static marmot.optor.AggregateFunction.*;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.CreateDataSetParameters;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAggregateByGroup {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
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
		
		DataSet input = marmot.getDataSet(INPUT);

		Plan plan = marmot.planBuilder("group_by")
							.load(INPUT)
							.groupBy("sig_cd")
								.aggregate(COUNT(), MAX("sub_sta_sn"), MIN("sub_sta_sn"),
											SUM("sub_sta_sn"), AVG("sub_sta_sn"),
											STDDEV("sub_sta_sn"))
							.store(RESULT)
							.build();
		
		CreateDataSetParameters params = CreateDataSetParameters.builder()
																.datasetId(RESULT)
																.initializer(plan, true)
																.force(true)
																.build();
		DataSet result = marmot.createDataSet(params);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedTimeString());
	}
}
