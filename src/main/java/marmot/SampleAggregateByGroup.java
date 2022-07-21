package marmot;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.StoreDataSetOptions.FORCE;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAggregateByGroup {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan = Plan.builder("group_by")
							.load(INPUT)
							.filter("sig_cd.startsWith('11')")
							.aggregateByGroup(Group.ofKeys("sig_cd"),
											COUNT(), MAX("sub_sta_sn"), MIN("sub_sta_sn"),
											SUM("sub_sta_sn"), AVG("sub_sta_sn"),
											STDDEV("sub_sta_sn"))
							.store(RESULT, FORCE)
							.build();
		marmot.execute(plan, ExecutePlanOptions.DISABLE_LOCAL_EXEC);
		
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 5);
	}
}
