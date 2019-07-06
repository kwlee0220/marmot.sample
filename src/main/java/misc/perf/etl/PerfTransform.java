package misc.perf.etl;


import static marmot.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordScript;
import marmot.command.MarmotClientCommands;
import marmot.optor.ParseCsvOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfTransform {
	private static final String INPUT_H0 = "교통/dtg_h0";
	private static final String INPUT_L0 = "교통/dtg_l0";
	private static final String INPUT_M0 = "교통/dtg_m0";
	private static final String INPUT_S0 = "교통/dtg_s0";
	private static final String INPUT_T0 = "교통/dtg_t0";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		collect(marmot, INPUT_T0, 5);
		collect(marmot, INPUT_S0, 5);
		collect(marmot, INPUT_M0, 5);
		collect(marmot, INPUT_L0, 5);
		collect(marmot, INPUT_H0, 5);
	}
	
	private static final void collect(MarmotRuntime marmot, String input, int count) {
		double avg = FStream.range(0, count)
							.map(idx -> process(marmot, input))
							.sort()
							.drop(1)
							.take(count - 2)
							.mapToLong(v -> v)
							.average()
							.get();
		long millis = Math.round(avg);
		System.out.printf("input=%s, elapsed=%s%n", input, UnitUtils.toSecondString(millis));
	}
	
	private static final long process(MarmotRuntime marmot, String input) {
		String planName = "perf_transform_" + input.replaceAll("/", ".");
		ParseCsvOptions opts = ParseCsvOptions.DEFAULT()
									.header("운행일자,운송사코드,차량번호,운행시분초,일일주행거리,누적주행거리,"
											+ "운행속도,RPM,브레이크신호,X좌표,Y좌표,방위각,가속도X,가속도Y");
		RecordScript tsExpr = RecordScript.of("$pat = DateTimePattern('yyyyMMddkkmmss')",
												"DateTimeParseLE(운행일자 + 운행시분초.substring(0,6), $pat)");
		Plan plan = marmot.planBuilder(planName)
						.load(input)
						.parseCsv("text", opts)
						.defineColumn("ts:datetime",tsExpr)
						.defineColumn("일일주행거리:int")
						.defineColumn("누적주행거리:int")
						.defineColumn("운행속도:short")
						.defineColumn("브레이크신호:byte")
						.defineColumn("RPM:short")
						.defineColumn("방위각:short")
						.toPoint("X좌표", "Y좌표", "the_geom")
						.project("the_geom, ts, *-{the_geom,X좌표,Y좌표,운행일자,운행시분초,ts}")
						.shard(1)
						.build();

		StopWatch watch = StopWatch.start();
		DataSet result = marmot.createDataSet("tmp/" + input, plan, FORCE.blockSize("64mb"));
		watch.stop();
		System.out.printf("\tcount=%d, elapsed=%s%n",
							result.getRecordCount(), watch.getElapsedSecondString());
		
		return watch.getElapsedInMillis();
	}
}
