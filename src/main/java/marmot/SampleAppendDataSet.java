package marmot;

import static marmot.optor.CreateDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAppendDataSet {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		DataSet input = marmot.getDataSet(INPUT);
		
		DataSet created = marmot.createDataSet(RESULT, input.getRecordSchema(),
												FORCE(input.getGeometryColumnInfo()));
		
		try ( RecordSet rset = input.read() ) {
			System.out.println("created: " + created.append(rset));
		}
		try ( RecordSet rset = input.read() ) {
			System.out.println("created: " + created.append(rset, "kafka"));
		}
		try ( RecordSet rset = input.read() ) {
			System.out.println("created: " + created.append(rset, "kafka"));
		}
		
		created = marmot.getDataSet(RESULT);
		System.out.println("total count=" + created.getRecordCount()
							+ ", elapsed=" + watch.stopAndGetElpasedTimeString());
	}
}
