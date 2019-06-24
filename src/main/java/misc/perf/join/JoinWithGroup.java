package misc.perf.join;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.plan.LoadOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JoinWithGroup {
	private static final String INPUT = "주소/건물POI";
	private static final String SAMPLE1 = "tmp/sample_1";
	private static final String SAMPLE3 = "tmp/sample_3";
	private static final String SAMPLE5 = "tmp/sample_5";
	private static final String SAMPLE7 = "tmp/sample_7";
	private static final String SAMPLE9 = "tmp/sample_9";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		collect(marmot, SAMPLE1, 5);
		collect(marmot, SAMPLE3, 5);
		collect(marmot, SAMPLE5, 5);
		collect(marmot, SAMPLE7, 5);
		collect(marmot, SAMPLE9, 5);
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
		String planName = "cluster_" + dsId.split("/")[1];
		GeometryColumnInfo gcInfo = marmot.getDataSet(dsId).getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder(planName)
					.load(dsId, LoadOptions.SPLIT_COUNT(2))
					.spatialJoin("the_geom", INPUT, "the_geom,param.출입구일련번호")
					.build();

		StopWatch watch = StopWatch.start();
		DataSet result = marmot.createDataSet("tmp/result", plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		watch.stop();
		System.out.printf("\tcount=%d, elapsed=%s%n",
							result.getRecordCount(), watch.getElapsedSecondString());
		
		return watch.getElapsedInMillis();
	}
}
