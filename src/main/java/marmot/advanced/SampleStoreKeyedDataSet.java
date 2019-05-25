package marmot.advanced;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.ExecutePlanOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
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
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("test StoreKeyedDataSet")
						.load(INPUT)
						.groupBy("sig_cd")
							.count()
//							.storeEachGroup(RESULT, DataSetOption.FORCE,
//											DataSetOption.GEOMETRY(gcInfo))
						.build();
//		marmot.execute(plan, ExecutePlanOption.DISABLE_LOCAL_EXEC);
		DataSet result = marmot.createDataSet(RESULT, plan, ExecutePlanOption.DISABLE_LOCAL_EXEC,
												StoreDataSetOptions.create().force(true));
		SampleUtils.printPrefix(result, 500);
	}
}
