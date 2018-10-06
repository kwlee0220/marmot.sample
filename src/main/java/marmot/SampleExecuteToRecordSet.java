package marmot;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClient;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExecuteToRecordSet {
	private static final String INPUT = "교통/지하철/서울역사";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();

		Plan plan = marmot.planBuilder("test")
							.load(INPUT)
							.filter("sub_sta_sn > 300 && sub_sta_sn < 310")
							.project("sub_sta_sn")
							.build();
		try ( RecordSet rset = marmot.executeToRecordSet(plan) ) {
			rset.forEach(System.out::println);
		}
	}
}
