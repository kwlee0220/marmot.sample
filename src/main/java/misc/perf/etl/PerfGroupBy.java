package misc.perf.etl;


import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfGroupBy {
	private static final String INPUT_L = "교통/dtg_l";
	private static final String INPUT_M = "교통/dtg_m";
	private static final String INPUT_S = "교통/dtg_s";
	
	private static final String INPUT_L1 = "교통/dtg_l1";
	private static final String INPUT_M1 = "교통/dtg_m1";
	private static final String INPUT_S1 = "교통/dtg_s1";
	
	private static final String INPUT_LE = "교통/dtg_le";
	private static final String INPUT_ME = "교통/dtg_me";
	private static final String INPUT_SE = "교통/dtg_se";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		collect(marmot, INPUT_SE, 1, 5);
		collect(marmot, INPUT_S1, 1, 5);
		collect(marmot, INPUT_S, 1, 5);	
		
		collect(marmot, INPUT_ME, 3, 5);
		collect(marmot, INPUT_M1, 3, 5);
		collect(marmot, INPUT_M, 3, 5);

		collect(marmot, INPUT_LE, 5, 5);
		collect(marmot, INPUT_L1, 5, 5);
		collect(marmot, INPUT_L, 5, 5);		
	}
	
	private static final void collect(MarmotRuntime marmot, String input, int nworkers, int count) {
		System.out.printf("GroupBy: input=%s...%n", input);
		double avg = FStream.range(0, count)
							.map(idx -> process(marmot, input, nworkers))
							.sort()
							.drop(1)
							.take(count - 2)
							.mapToLong(v -> v)
							.average()
							.get();
		long millis = Math.round(avg);
		System.out.printf("elapsed=%s%n%n", UnitUtils.toSecondString(millis));
	}
	
	private static final long process(MarmotRuntime marmot, String input, int nworkers) {
		String planName = "perf_group_by_" + input.replaceAll("/", ".");
		Plan plan = marmot.planBuilder(planName)
							.load(input)
							.filter("운행속도 > 80")
							.defineColumn("hour:byte", "DateTimeGetHour(ts)")
							.filter("hour >= 8 && hour <= 10")
							.groupBy("차량번호")
								.aggregate(AggregateFunction.COUNT())
							.build();

		StopWatch watch = StopWatch.start();
		DataSet result = marmot.createDataSet("tmp/result", plan, DataSetOption.FORCE);
		watch.stop();
		System.out.printf("\tcount=%d, elapsed=%s%n",
							result.getRecordCount(), watch.getElapsedSecondString());
		
		return watch.getElapsedInMillis();
	}
}
