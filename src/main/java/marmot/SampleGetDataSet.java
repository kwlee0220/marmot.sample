package marmot;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleGetDataSet {
	private static final String INPUT = "POI/주유소_가격";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		for ( DataSet ds: marmot.getDataSetAllInDir("구역", true) ) {
			System.out.println(ds.getId());
		}
		
		DataSet input = marmot.getDataSet(INPUT);
		int cnt = (int)input.getRecordCount();
		
		try ( RecordSet rset = input.read() ) {
			Record record;
			while ( (record = rset.nextCopy()) != null ) {
				System.out.printf("상호: %s, 휘발유: %s%n",
									record.getString("상호"),
									record.getString("휘발유"));
			}
		}
	}
}
