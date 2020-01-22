package marmot.analysis;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleGetAnalysisParameters {
	private static final String INPUT = "주소/건물POI";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

//		marmot.deleteAnalysisAll();
		
		for ( String id: marmot.getSystemAnalysisClassIdAll() ) {
			System.out.println("system analysis: " + id);
			System.out.println("\targs: " + marmot.getSystemAnalysisParameterNameAll(id));
		}
		
		for ( String id: marmot.getModuleAnalysisClassIdAll() ) {
			System.out.println("module analysis: " + id);
			System.out.println("\targs: " + marmot.getModuleAnalysisParameterNameAll(id));
		}
	}
}
