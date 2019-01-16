package marmot.geom.advanced;

import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.advanced.LISAWeight;
import marmot.remote.protobuf.PBMarmotClient;
import utils.stream.KVFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFindLocalMoranI {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "시연/대전공장";
	private static final String VALUE_COLUMN = "FCTR_MEAS";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan0 = marmot.planBuilder("find_statistics")
								.load(INPUT)
								.aggregate(AggregateFunction.COUNT(),
											AggregateFunction.AVG(VALUE_COLUMN),
											AggregateFunction.STDDEV(VALUE_COLUMN))
								.build();
		
		Record result = marmot.executeToRecord(plan0).get();
		Map<String,Object> params = KVFStream.of(result.toMap())
											.mapKey((k,v) -> k.get())
											.toMap();
		double avg = (Double)params.get("avg");
		Plan plan1 = marmot.planBuilder("find_statistics2")
							.load(INPUT)
							.defineColumn("diff:double", "fctr_meas -" + avg)
							.expand("diff2:double,diff4:double",
									"diff2 = diff * diff; diff4=diff2*diff2")
							.aggregate(AggregateFunction.SUM("diff").as("diffSum"),
										AggregateFunction.SUM("diff2").as("diff2Sum"),
										AggregateFunction.SUM("diff4").as("diff4Sum"))
							.build();

		RecordSet result1 = marmot.executeToRecordSet(plan1);
		KVFStream.of(result1.getFirst().toMap())
				.mapKey((k,v) -> k.get())
				.toMap(params);
		
		Plan plan = marmot.planBuilder("local_spatial_auto_correlation")
								.loadLocalMoranI(INPUT, "uid", "fctr_meas", 1000,
												LISAWeight.FIXED_DISTANCE_BAND)
								.project("uid,moran_i,moran_zscore,moran_pvalue")
								.sort("UID")
								.store(RESULT)
								.build();
		DataSet result3 = marmot.createDataSet(RESULT, plan, DataSetOption.FORCE);
		SampleUtils.printPrefix(result3, 5);
	}
}
