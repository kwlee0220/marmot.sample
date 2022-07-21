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
public class SampleFindFirstByGroup {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan = Plan.builder("group_by")
							.load(INPUT)
//							.filter("sig_cd.startsWith('11')")
							.takeByGroup(Group.ofKeys("sig_cd").orderBy("SUB_STA_SN:D"), 1)
							.project("* - {the_geom}")
							.store(RESULT, FORCE)
							.build();
		marmot.execute(plan, ExecutePlanOptions.DISABLE_LOCAL_EXEC);
		
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 10);
	}
}
