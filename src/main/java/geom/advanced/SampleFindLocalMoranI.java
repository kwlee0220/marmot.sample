package geom.advanced;

import java.util.Map;
import java.util.UUID;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Maps;

import common.SampleUtils;
import marmot.Plan;
import marmot.Record;
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
		
		String tempPath = "tmp/" + UUID.randomUUID();
		Plan plan0 = marmot.planBuilder("find_statistics")
								.load(INPUT)
								.aggregate(AggregateFunction.COUNT(),
											AggregateFunction.AVG(VALUE_COLUMN),
											AggregateFunction.STDDEV(VALUE_COLUMN))
								.storeMarmotFile(tempPath)
								.build();
		marmot.execute(plan0);
		
		Map<String,Object> params = Maps.newHashMap();

		Record result = marmot.readMarmotFile(tempPath).stream().findAny().get();
		params.putAll(result.toMap());
		marmot.deleteFile(tempPath);
		
		double avg = (Double)params.get("avg");
		Plan plan1 = marmot.planBuilder("find_statistics2")
							.load(INPUT)
							.expand1("diff:double", "fctr_meas -" + avg)
							.expand("diff2:double,diff4:double",
									"diff2 = diff * diff; diff4=diff2*diff2")
							.aggregate(AggregateFunction.SUM("diff").as("diffSum"),
										AggregateFunction.SUM("diff2").as("diff2Sum"),
										AggregateFunction.SUM("diff4").as("diff4Sum"))
							.storeMarmotFile(tempPath)
							.build();
		marmot.execute(plan1);
		Record result2 = marmot.readMarmotFile(tempPath).stream().findAny().get();
		params.putAll(result2.toMap());
		marmot.deleteFile(tempPath);
		
		Plan plan = marmot.planBuilder("local_spatial_auto_correlation")
								.loadLocalMoranI(INPUT, "uid", "fctr_meas", 1000,
												LISAWeight.FIXED_DISTANCE_BAND)
								.project("uid,moran_i,moran_zscore,moran_pvalue")
								.sort("UID")
								.storeMarmotFile(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 5);
	}
}
