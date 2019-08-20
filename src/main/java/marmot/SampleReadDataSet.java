package marmot;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.DefaultRecord;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleReadDataSet {
//	private static final String INPUT = "구역/연속지적도_2019";
	private static final String INPUT = "교통/지하철/서울역사";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		DataSet input = marmot.getDataSet(INPUT);
		
		int count = 0;
		try ( RecordSet rset = input.read() ) {
			Record record = DefaultRecord.of(rset.getRecordSchema());
			while ( rset.next(record) ) {
				if ( ++count % 1000000 == 0 ) {
					System.out.println(count);
				}
			}
		}
		
		System.out.println("total count=" + count + ", elapsed=" + watch.stopAndGetElpasedTimeString());
	}
}
