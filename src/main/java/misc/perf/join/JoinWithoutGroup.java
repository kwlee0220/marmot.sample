package misc.perf.join;

import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.LoadOptions;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JoinWithoutGroup {
	private static final String INPUT = "주소/건물POI";
	private static final String SAMPLE1 = "tmp/sample_1";
	private static final String SAMPLE3 = "tmp/sample_3";
	private static final String SAMPLE5 = "tmp/sample_5";
	private static final String SAMPLE7 = "tmp/sample_7";
	private static final String SAMPLE9 = "tmp/sample_9";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
//		collect(marmot, SAMPLE1, 5);
//		collect(marmot, SAMPLE3, 5);
//		collect(marmot, SAMPLE5, 5);
		collect(marmot, SAMPLE7, 1);
		collect(marmot, SAMPLE9, 1);
	}
	
	private static final void collect(MarmotRuntime marmot, String input, int count) {
		System.out.printf("join_without_cluster: input=%s...%n", input);
		
		long millis;
		if ( count > 1 ) {
			double avg = FStream.range(0, count)
								.map(idx -> join(marmot, input))
								.sort()
								.drop(1)
								.take(count - 2)
								.mapToLong(v -> v)
								.average()
								.get();
			millis = Math.round(avg);
		}
		else {
			millis = join(marmot, input);
		}
		System.out.printf("elapsed=%s%n%n", UnitUtils.toSecondString(millis));
	}
	
	private static long join(MarmotRuntime marmot, String dsId) {
		String planName = "no_cluster_" + dsId.split("/")[1];
		GeometryColumnInfo gcInfo = marmot.getDataSet(dsId).getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder(planName)
					.load(dsId, LoadOptions.SPLIT_COUNT(2))
					.spatialJoin("the_geom", INPUT, "the_geom,param.출입구일련번호",
								SpatialJoinOptions.DEFAULT.clusterOuterRecords(false))
					.store("tmp/result", FORCE(gcInfo))
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
