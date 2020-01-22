package testcase.analysis;

import org.apache.log4j.PropertyConfigurator;

import marmot.analysis.module.NormalizeParameters;
import marmot.command.MarmotClientCommands;
import marmot.exec.ModuleAnalysis;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleModuleAnalysis {
	private static final String INPUT = "tmp/input";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(INPUT);
		params.outputDataset(RESULT);
		params.inputFeatureColumns("휘발유");
		params.outputFeatureColumns("normalized");
		ModuleAnalysis anal = new ModuleAnalysis("휘발유_표준화", "normalize", params.toMap());
		marmot.addAnalysis(anal, true);
	}
}
