package misc.perf.etl;


import static marmot.optor.StoreDataSetOptions.FORCE;

import org.locationtech.jts.geom.Envelope;

import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfRangeQuery {
	private static final String EMD = "구역/읍면동";
	private static final String SGG = "구역/시군구";
	private static final String SIDO = "구역/시도";
	
	private static final String INPUT_L = "교통/dtg_l";
	private static final String INPUT_M = "교통/dtg_m";
	private static final String INPUT_S = "교통/dtg_s";
	
	private static final String INPUT_LG = "교통/dtg_lg";
	private static final String INPUT_MG = "교통/dtg_mg";
	private static final String INPUT_SG = "교통/dtg_sg";
	
	private static final String INPUT_LE = "교통/dtg_le";
	private static final String INPUT_ME = "교통/dtg_me";
	private static final String INPUT_SE = "교통/dtg_se";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Envelope range = getSeoChoDong(marmot);

//		collect(marmot, INPUT_S, range, 5);
//		collect(marmot, INPUT_M, range, 5);
//		collect(marmot, INPUT_L, range, 5);
//		
//		collect(marmot, INPUT_SE, range, 5);
//		collect(marmot, INPUT_ME, range, 5);
//		collect(marmot, INPUT_LE, range, 5);

		collect(marmot, INPUT_SG, range, 5);
		collect(marmot, INPUT_MG, range, 5);
		collect(marmot, INPUT_LG, range, 5);
	}
	
	private static final void collect(MarmotRuntime marmot, String input, Envelope range, int count) {
		double avg = FStream.range(0, count)
							.map(idx -> process(marmot, input, range))
							.sort()
							.drop(1)
							.take(count - 2)
							.mapToLong(v -> v)
							.average()
							.get();
		long millis = Math.round(avg);
		System.out.printf("input=%s, elapsed=%s%n", input, UnitUtils.toSecondString(millis));
	}
	
	private static final long process(MarmotRuntime marmot, String input, Envelope range) {
		String planName = "perf_range_query_" + input.replaceAll("/", ".");
		Plan plan = Plan.builder(planName)
							.query(input, range)
							.buffer("the_geom", 100)
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
	
	private static Envelope getSeoChoDong(MarmotRuntime marmot) {
		Plan plan = Plan.builder("get seochodong")
							.load(SIDO)
							.filter("ctprvn_cd == '41' || ctprvn_cd == '28' || ctprvn_cd == '11'")
							.project("the_geom")
							.build();
		try ( RecordSet rset = marmot.executeLocally(plan) ) {
			return rset.fstream().map(r -> r.getGeometry("the_geom"))
						.map(g -> g.getEnvelopeInternal())
						.collect(new Envelope(), (c,v) -> c.expandToInclude(v));
		}
	}
}
