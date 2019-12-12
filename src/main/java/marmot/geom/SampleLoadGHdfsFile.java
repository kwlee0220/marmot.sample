package marmot.geom;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadGHdfsFile {
	private static final String LAYER = "pop_age_interval_2000";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		plan = marmot.planBuilder("load_text")
					.loadGHdfsFile(LAYER)
					.filter("age_intvl==20")
//					.aggregate(AggregateFunction.COUNT())
					.store("tmp/result", FORCE)
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
	}
}
