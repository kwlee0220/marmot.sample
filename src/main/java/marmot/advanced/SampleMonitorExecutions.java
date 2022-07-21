package marmot.advanced;

import marmot.command.MarmotClientCommands;
import marmot.exec.MarmotExecution;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleMonitorExecutions {
	private static final String INPUT = "주소/건물POI";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		for ( int i =0; i < 1000; ++i ) {
			int idx = 0;
			for ( MarmotExecution exec: marmot.getMarmotExecutionAll() ) {
				System.out.printf("%02d: %s%n", idx, exec);
				++idx;
			}
			System.out.println("--------------------------------------");
			
			Thread.sleep(10 * 1000);
		}
	}
}
