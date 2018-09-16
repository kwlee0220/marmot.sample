package basic;

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
public class SampleSort {
//	private static final String INPUT = "POI/주유소_가격";
	private static final String INPUT = "POI/전국cctv";
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
		Plan plan = marmot.planBuilder("sample_aggreate")
							.load(INPUT)
//							.filter("보관일수 > 0")
							.sort("보관일수:A:F,카메라대수:A")
							.project("the_geom,관리기관명,보관일수,카메라대수")
							.store(RESULT)
							.build();
		CreateDataSetParameters params = new CreateDataSetParameters(RESULT, plan, true)
												.setGeometryColumnInfo(input.getGeometryColumnInfo())
												.setForce();
		DataSet result = marmot.createDataSet(params);
		watch.stop();

		SampleUtils.printPrefix(result, 50);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
