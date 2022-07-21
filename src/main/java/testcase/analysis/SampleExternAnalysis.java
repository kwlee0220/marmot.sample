package testcase.analysis;

import marmot.command.MarmotClientCommands;
import marmot.exec.ExternAnalysis;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExternAnalysis {
	private static final String INPUT = "교통/지하철/서울역사";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		String[] externArgs = new String[] { "cluster", "delete", INPUT };
		ExternAnalysis anal = new ExternAnalysis("색인_삭제", "mc_dataset", externArgs);
		marmot.addAnalysis(anal, true);
	}
}
