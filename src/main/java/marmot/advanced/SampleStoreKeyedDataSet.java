package marmot.advanced;


import static marmot.ExecutePlanOptions.DISABLE_LOCAL_EXEC;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
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
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("test StoreKeyedDataSet")
						.load(INPUT)
						.aggregateByGroup(Group.ofKeys("sig_cd"), AggregateFunction.COUNT())
//						.storeEachGroup(RESULT, DataSetOption.FORCE,
//										DataSetOption.GEOMETRY(gcInfo))
						.build();
//		marmot.execute(plan, ExecutePlanOption.DISABLE_LOCAL_EXEC);
		DataSet result = marmot.createDataSet(RESULT, plan, DISABLE_LOCAL_EXEC,
												StoreDataSetOptions.FORCE);
		SampleUtils.printPrefix(result, 500);
	}
}
