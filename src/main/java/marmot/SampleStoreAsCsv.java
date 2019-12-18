package marmot;

import static marmot.optor.StoreAsCsvOptions.DEFAULT;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleStoreAsCsv {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	private static final String RESULT2 = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan;
		plan = Plan.builder("test")
					.load(INPUT)
					.filter("sub_sta_sn > 320")
					.project("kor_sub_nm,sub_sta_sn,sig_cd")
					.storeAsCsv(RESULT, DEFAULT('@'))
					.build();
		marmot.execute(plan);
		
		plan = Plan.builder("test")
					.load(INPUT)
					.filter("sub_sta_sn > 320")
					.project("kor_sub_nm,sub_sta_sn,sig_cd")
					.storeAsCsv(RESULT2, DEFAULT(',').compressionCodecName("gzip"))
					.build();
		marmot.execute(plan);
	}
}
