package marmot;

import static marmot.DataSetOption.FORCE;
import static marmot.optor.AggregateFunction.*;
import static marmot.optor.AggregateFunction.ENVELOPE;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.plan.STScriptPlanLoader;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAggregate {
	private static final String INPUT = "POI/주유소_가격";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan = marmot.planBuilder("sample_aggreate")
							.load(INPUT)
							.filter("휘발유 > 0")
							.aggregate(COUNT(), MAX("휘발유"), MIN("휘발유"), AVG("휘발유"), STDDEV("휘발유"),
										ENVELOPE("the_geom"))
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE);
		SampleUtils.printPrefix(result, 5);
	}
}
