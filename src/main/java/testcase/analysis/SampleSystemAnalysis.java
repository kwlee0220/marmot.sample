package testcase.analysis;

import org.apache.log4j.PropertyConfigurator;

import marmot.analysis.system.SystemAnalysis;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSystemAnalysis {
	private static final String INPUT = "교통/지하철/서울역사";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		SystemAnalysis analy = SystemAnalysis.clusterDataSet("서울역사_색인", INPUT);
		marmot.addAnalysis(analy, true);
	}
}
