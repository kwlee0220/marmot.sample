package basic;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.STDDEV;
import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.CreateDataSetParameters;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotClient;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAggregateByGroupMR {
	private static final String INPUT = "POI/주유소_가격";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();

		Plan plan = marmot.planBuilder("group_by")
							.load(INPUT)
							.groupBy("지역")
								.workerCount(2)
								.aggregate(COUNT(), MAX("휘발유"), MIN("휘발유"),
											SUM("휘발유"), AVG("휘발유"),
											STDDEV("휘발유"))
							.store(RESULT)
							.build();
		
		CreateDataSetParameters params = new CreateDataSetParameters(RESULT, plan, true)
																.setForce();
		DataSet result = marmot.createDataSet(params);
		SampleUtils.printPrefix(result, 5);
	}
}
