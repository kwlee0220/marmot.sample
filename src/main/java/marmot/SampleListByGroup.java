package marmot;

import static marmot.DataSetOption.FORCE;
import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;
import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleListByGroup {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan = marmot.planBuilder("group_by")
							.load(INPUT)
							.filter("sig_cd.startsWith('11')")
							.groupBy("sig_cd")
								.orderBy("sub_sta_sn:A")
								.list()
							.project("sig_cd, sub_sta_sn")
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE);
		SampleUtils.printPrefix(result, 20);
	}
}
