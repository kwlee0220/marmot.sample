package marmot.advanced;


import static marmot.ExecutePlanOptions.DISABLE_LOCAL_EXEC;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleStoreKeyedDataSet {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		Plan plan = Plan.builder("test StoreKeyedDataSet")
						.load(INPUT)
						.storeByGroup(Group.ofKeys("sig_cd"), RESULT, StoreDataSetOptions.FORCE(gcInfo))
						.build();
		marmot.execute(plan, DISABLE_LOCAL_EXEC);
		
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 500);
	}
}
