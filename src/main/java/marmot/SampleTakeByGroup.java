package marmot;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleTakeByGroup {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan = marmot.planBuilder("group_by")
							.load(INPUT)
							.filter("sig_cd.startsWith('11')")
							.takeByGroup(Group.ofKeys("sig_cd")
											.orderBy("sub_sta_sn:A"), 2)
							.project("sig_cd, sub_sta_sn")
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().force(true));
		SampleUtils.printPrefix(result, 20);
	}
}
