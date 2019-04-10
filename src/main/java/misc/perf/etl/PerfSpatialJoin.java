package misc.perf.etl;


import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
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
public class PerfSpatialJoin {
	private static final String EMD = "구역/읍면동";
	private static final String REF = "tmp/emd";

	private static final String INPUT_S = "교통/dtg_s";
	private static final String INPUT_M = "교통/dtg_m";
	private static final String INPUT_L = "교통/dtg_l";
	
	private static final String INPUT_L2 = "교통/dtg_l2";
	private static final String INPUT_M2 = "교통/dtg_m2";
	private static final String INPUT_S2 = "교통/dtg_s2";
	
	private static final String INPUT_SE = "교통/dtg_se";
	private static final String INPUT_ME = "교통/dtg_me";
	private static final String INPUT_LE = "교통/dtg_le";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		transformEmd(marmot);

		collect(marmot, INPUT_SE, 3, 5);
		collect(marmot, INPUT_S2, 3, 5);
		collect(marmot, INPUT_S, 3, 5);

		collect(marmot, INPUT_ME, 5, 5);
		collect(marmot, INPUT_M2, 5, 5);
		collect(marmot, INPUT_M, 5, 5);
		
		collect(marmot, INPUT_LE, 7, 5);
		collect(marmot, INPUT_L2, 7, 5);
		collect(marmot, INPUT_L, 7, 5);
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
							.defineColumn("hour:byte", "DateTimeGetHour(ts)")
							.spatialJoin("the_geom", REF, "hour,운행속도,param.emd_cd")
							.groupBy("hour,emd_cd")
								.workerCount(nworkers)
								.aggregate(AggregateFunction.MAX("운행속도"))
							.build();

		StopWatch watch = StopWatch.start();
		DataSet result = marmot.createDataSet("tmp/result", plan, FORCE);
		watch.stop();
		System.out.printf("\tcount=%d, elapsed=%s%n",
							result.getRecordCount(), watch.getElapsedSecondString());
		
		return watch.getElapsedInMillis();
	}
	
	private static final void transformEmd(MarmotRuntime marmot) {
		Plan plan = marmot.planBuilder("transform_srid")
						.load(EMD)
						.transformCrs("the_geom", "EPSG:5186", "EPSG:4326")
						.build();

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:4326");
		DataSet result = marmot.createDataSet(REF, plan, GEOMETRY(gcInfo), FORCE);
		result.cluster();
	}
}
