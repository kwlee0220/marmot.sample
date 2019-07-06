package marmot.geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.geo.advanced.LISAWeight;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFindGetisOrdGiStar {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "시연/대전공장";
	private static final String VALUE_COLUMN = "FCTR_MEAS";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan = marmot.planBuilder("local_spatial_auto_correlation")
								.loadGetisOrdGi(INPUT, VALUE_COLUMN, 1000,
												LISAWeight.FIXED_DISTANCE_BAND)
								.project("UID,gi_zscore,gi_pvalue")
								.sort("UID")
								.build();
		DataSet result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.FORCE);
		SampleUtils.printPrefix(result, 5);
	}
}
