package service_area;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotCommands;
import marmot.process.geo.ServiceAreaAnaysisParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestServiceAreaAnalysis {
	private static final String INPUT = "교통/지하철/출입구";
	private static final String NETWORK = "교통/도로/네트워크_fixed";
	private static final String RESULT = "tmp/service_area/result";
	
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
		
		StopWatch total = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		ServiceAreaAnaysisParameters params = new ServiceAreaAnaysisParameters();
		params.inputDataset(INPUT);
		params.networkDataset(NETWORK);
		params.outputDataset(RESULT);
		params.serviceDistance(1000);
		params.initialRadius(50);
		
		marmot.executeProcess("service_area_analysis", params.toMap());
		
		System.out.printf("서비스 영역 분석 완료, elapsed: %s%n",
							total.stopAndGetElpasedTimeString());
	}
}
