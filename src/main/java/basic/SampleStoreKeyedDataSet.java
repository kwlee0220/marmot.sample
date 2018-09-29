package basic;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotClient;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleStoreKeyedDataSet {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		Plan plan = marmot.planBuilder("test StoreKeyedDataSet")
							.load(INPUT)
							.groupBy("sig_cd")
								.storeEachGroup(RESULT, input.getGeometryColumnInfo())
							.build();
		marmot.deleteDir(RESULT);
		marmot.execute(plan);
	}
}
