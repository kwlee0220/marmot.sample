package marmot;

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
public class SampleListByGroup {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan = Plan.builder("group_by")
							.load(INPUT)
							.filter("sig_cd.startsWith('11')")
							.listByGroup(Group.ofKeys("sig_cd").orderBy("sub_sta_sn:A"))
							.project("sig_cd, sub_sta_sn")
							.store(RESULT, FORCE)
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 20);
	}
}
