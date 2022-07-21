package marmot;

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
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		for ( DataSet ds: marmot.getDataSetAll() ) {
			System.out.println(ds.getId());
		}
		
//		for ( DataSet ds: marmot.getDataSetAllInDir("POI", true) ) {
//			System.out.println(ds.getId());
//		}
		
		DataSet input = marmot.getDataSet(INPUT);
		int cnt = (int)input.getRecordCount();
		
//		try ( RecordSet rset = input.read() ) {
//			Record record;
//			while ( (record = rset.nextCopy()) != null ) {
//				System.out.printf("상호: %s, 휘발유: %s%n",
//									record.getString("상호"),
//									record.getString("휘발유"));
//			}
//		}
	}
}
