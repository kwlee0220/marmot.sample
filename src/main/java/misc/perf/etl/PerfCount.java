package misc.perf.etl;


import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfCount {
	private static final String INPUT_H = "교통/dtg_h";
	private static final String INPUT_H0 = "교통/dtg_h0";
	private static final String INPUT_HE = "교통/dtg_he";
	
	private static final String INPUT_L = "교통/dtg_l";
	private static final String INPUT_L0 = "교통/dtg_l0";
	private static final String INPUT_LE = "교통/dtg_le";
	
	private static final String INPUT_M = "교통/dtg_m";
	private static final String INPUT_M0 = "교통/dtg_m0";
	private static final String INPUT_ME = "교통/dtg_me";
	
	private static final String INPUT_S = "교통/dtg_s";
	private static final String INPUT_S0 = "교통/dtg_s0";
	private static final String INPUT_SE = "교통/dtg_se";
	
	private static final String INPUT_T = "교통/dtg_t";
	private static final String INPUT_T0 = "교통/dtg_t0";
	private static final String INPUT_TE = "교통/dtg_te";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		collect(marmot, INPUT_TE, 5);
		collect(marmot, INPUT_T0, 5);
		collect(marmot, INPUT_T, 5);

		collect(marmot, INPUT_SE, 5);
		collect(marmot, INPUT_S0, 5);
		collect(marmot, INPUT_S, 5);

		collect(marmot, INPUT_ME, 5);
		collect(marmot, INPUT_M0, 5);
		collect(marmot, INPUT_M, 5);

		collect(marmot, INPUT_LE, 5);
		collect(marmot, INPUT_L0, 5);
		collect(marmot, INPUT_L, 5);

		collect(marmot, INPUT_HE, 5);
		collect(marmot, INPUT_H0, 5);
		collect(marmot, INPUT_H, 5);		
	}
	
	private static final void collect(MarmotRuntime marmot, String input, int count) {
		System.out.printf("Count: input=%s...%n", input);
		double avg = FStream.range(0, count)
							.map(idx -> process(marmot, input))
							.sort()
							.drop(1)
							.take(count - 2)
							.mapToLong(v -> v)
							.average()
							.get();
		long millis = Math.round(avg);
		System.out.printf("elapsed=%s%n%n", UnitUtils.toSecondString(millis));
	}
	
	private static final long process(MarmotRuntime marmot, String input) {
		String planName = "perf_count_" + input.replaceAll("/", ".");
		Plan plan = Plan.builder(planName)
							.load(input)
							.aggregate(AggregateFunction.COUNT())
							.store("tmp/result", FORCE)
							.build();

		StopWatch watch = StopWatch.start();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet("tmp/result");
		watch.stop();
		System.out.printf("\tcount=%d, elapsed=%s%n",
							result.getRecordCount(), watch.getElapsedSecondString());
		
		return watch.getElapsedInMillis();
	}
}
