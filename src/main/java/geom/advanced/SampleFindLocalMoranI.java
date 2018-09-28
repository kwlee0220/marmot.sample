package geom.advanced;

import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Maps;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.LISAWeight;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

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
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		Plan plan0 = marmot.planBuilder("find_statistics")
								.load(INPUT)
								.aggregate(AggregateFunction.COUNT(),
											AggregateFunction.AVG(VALUE_COLUMN),
											AggregateFunction.STDDEV(VALUE_COLUMN))
								.build();

		Map<String,Object> params = Maps.newHashMap();
		RecordSet result0 = marmot.executeWithTemporaryDataSet(plan0);
		params.putAll(result0.getFirst().get().toMap());
		
		double avg = (Double)params.get("avg");
		Plan plan1 = marmot.planBuilder("find_statistics2")
							.load(INPUT)
							.expand1("diff:double", "fctr_meas -" + avg)
							.expand("diff2:double,diff4:double",
									"diff2 = diff * diff; diff4=diff2*diff2")
							.aggregate(AggregateFunction.SUM("diff").as("diffSum"),
										AggregateFunction.SUM("diff2").as("diff2Sum"),
										AggregateFunction.SUM("diff4").as("diff4Sum"))
							.build();

		RecordSet result1 = marmot.executeWithTemporaryDataSet(plan1);
		params.putAll(result1.getFirst().get().toMap());
		
		Plan plan = marmot.planBuilder("local_spatial_auto_correlation")
								.loadLocalMoranI(INPUT, "uid", "fctr_meas", 1000,
												LISAWeight.FIXED_DISTANCE_BAND)
								.project("uid,moran_i,moran_zscore,moran_pvalue")
								.sort("UID")
								.store(RESULT)
								.build();
		DataSet result3 = marmot.createDataSet(RESULT, plan, DataSetOption.FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result3, 5);
	}
}
