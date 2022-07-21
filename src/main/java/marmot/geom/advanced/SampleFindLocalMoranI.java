package marmot.geom.advanced;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.Map;

import com.google.common.collect.Maps;

import common.SampleUtils;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.advanced.LISAWeight;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFindLocalMoranI {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "시연/대전공장";
	private static final String VALUE_COLUMN = "FCTR_MEAS";

	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan0 = Plan.builder("find_statistics")
								.load(INPUT)
								.aggregate(AggregateFunction.COUNT(),
											AggregateFunction.AVG(VALUE_COLUMN),
											AggregateFunction.STDDEV(VALUE_COLUMN))
								.build();
		
		Record result = marmot.executeToRecord(plan0).get();
		Map<String,Object> params = Maps.newHashMap(result.toMap());
		double avg = (Double)params.get("avg");
		Plan plan1 = Plan.builder("find_statistics2")
							.load(INPUT)
							.defineColumn("diff:double", "fctr_meas -" + avg)
							.expand("diff2:double,diff4:double",
									"diff2 = diff * diff; diff4=diff2*diff2")
							.aggregate(AggregateFunction.SUM("diff").as("diffSum"),
										AggregateFunction.SUM("diff2").as("diff2Sum"),
										AggregateFunction.SUM("diff4").as("diff4Sum"))
							.build();

		RecordSet result1 = marmot.executeToRecordSet(plan1);
		params.putAll(result1.findFirst().toMap());
		
		Plan plan = Plan.builder("local_spatial_auto_correlation")
								.loadLocalMoranI(INPUT, "uid", "fctr_meas", 1000,
												LISAWeight.FIXED_DISTANCE_BAND)
								.project("uid,moran_i,moran_zscore,moran_pvalue")
								.sort("UID")
								.store(RESULT, FORCE)
								.build();
		marmot.execute(plan);
		DataSet result3 = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result3, 5);
	}
}
