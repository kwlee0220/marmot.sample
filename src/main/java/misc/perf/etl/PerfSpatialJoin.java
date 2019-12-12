package misc.perf.etl;


import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfSpatialJoin {
	private static final String EMD = "구역/읍면동";
	private static final String HJD = "구역/행정동코드";
	private static final String ELDERLY = "POI/노인복지시설";
	private static final String REF = "tmp/emd";

	private static final String INPUT_T = "교통/dtg_t";
	private static final String INPUT_S = "교통/dtg_s";
	private static final String INPUT_M = "교통/dtg_m";
	private static final String INPUT_L = "교통/dtg_l";
	private static final String INPUT_H = "교통/dtg_h";

	private static final String INPUT_H2 = "교통/dtg_h2";
	private static final String INPUT_L2 = "교통/dtg_l2";
	private static final String INPUT_M2 = "교통/dtg_m2";
	private static final String INPUT_S2 = "교통/dtg_s2";
	private static final String INPUT_T2 = "교통/dtg_t2";

	private static final String INPUT_TE = "교통/dtg_te";
	private static final String INPUT_SE = "교통/dtg_se";
	private static final String INPUT_ME = "교통/dtg_me";
	private static final String INPUT_LE = "교통/dtg_le";
	private static final String INPUT_HE = "교통/dtg_he";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		collect(marmot, INPUT_TE, 1, 5);
		collect(marmot, INPUT_T, 1, 5);

		collect(marmot, INPUT_SE, 3, 5);
		collect(marmot, INPUT_S, 3, 5);

		collect(marmot, INPUT_ME, 5, 5);
		collect(marmot, INPUT_M, 5, 5);
		
		collect(marmot, INPUT_LE, 7, 5);
		collect(marmot, INPUT_L, 7, 5);
		
		collect(marmot, INPUT_HE, 7, 5);
		collect(marmot, INPUT_H, 7, 5);
	}
	
	private static final void collect(MarmotRuntime marmot, String input, int nworkers, int count) {
		System.out.printf("SpatialJoin: input=%s...%n", input);
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
		String planName = "perf_spatial_join_" + input.replaceAll("/", ".");
		Plan plan = marmot.planBuilder(planName)
							.load(input)
							.transformCrs("the_geom", "EPSG:4326", "EPSG:5186")
							.buffer("the_geom", 100)
							.defineColumn("hour:byte", "DateTimeGetHour(ts)")
							.spatialJoin("the_geom", ELDERLY,
										"param.{the_geom,bplc_nm,row_num},hour,운행속도")
							.spatialJoin("the_geom", HJD, "*-{the_geom},param.hcode")
							.aggregateByGroup(Group.ofKeys("bplc_nm,hour").withTags("hcode"),
												AggregateFunction.MAX("운행속도"))
							.project("bplc_nm,hcode,hour,max")
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
