package marmot.geom;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordScript;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.func.KeyValue;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleRangeQuery {
	private static final String RESULT = "tmp/result";
	private static final String SIDO = "구역/시도";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
//	private static final String BUILDINGS = "주소/건물POI";
//	private static final String INPUT = "주소/건물POI_clustered";
//	private static final String INPUT = "교통/dtg_201609_clustered";
//	private static final String INPUT = "구역/집계구";
//	private static final String INPUT = "토지/용도지역지구";
	private static final String INPUT = "건물/건물통합정보마스터";
	private static final String INPUT2 = INPUT + "_clustered";
	private static final String INPUT3 = INPUT + "_idxed";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		Plan plan = Plan.builder("sample_rangequery")
						.load(SGG)
						.project("the_geom,SIG_KOR_NM")
						.build();
		List<KeyValue<String,Geometry>> result
						= marmot.executeToRecordSet(plan).fstream()
								.map(r -> KeyValue.of(r.getString(1), r.getGeometry(0)))
								.toList();
		int remains = 70;
		for ( KeyValue<String,Geometry> kv: result ) {
//			if ( --remains > 0 ) {
//				continue;
//			}
//			if ( !kv.key().equals("산청군") ) {
//				continue;
//			}
			System.out.printf("%s: ", kv.key());
			Envelope range = kv.value().getEnvelopeInternal();
			
//			StopWatch watch1 = StopWatch.start();
//			long count1 = countSequenceFile(marmot, range);
//			watch1.stop();
//			System.out.printf("seq=%s (%s)", count1, watch1.getElapsedMillisString());
			
//			StopWatch watch2 = StopWatch.start();
//			long count2 = countSequenceFile2(marmot, range);
//			watch2.stop();
//			System.out.printf(", seq2=%s (%s)", count2, watch2.getElapsedMillisString());
			
			StopWatch watch3 = StopWatch.start();
			long count3 = countSpatialCluster(marmot, range);
			watch3.stop();
			System.out.printf(", cluster=%s (%s)", count3, watch3.getElapsedMillisString());
			
			StopWatch watch4 = StopWatch.start();
			long count4 = countSpatialIndex(marmot, range);
			watch4.stop();
			System.out.printf(", index=%s (%s)", count4, watch4.getElapsedMillisString());
			
			if ( count4 != count3 || count3 != count4 ) {
				System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			}
			
			System.out.println();
		}
		
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static long countSequenceFile(MarmotRuntime marmot, Envelope range) {
		DataSet input = marmot.getDataSet(INPUT);
		
		String expr = String.format("ST_Intersects(ST_AsEnvelope(%s),$envl)",
									input.getGeometryColumn());
		RecordScript predicate = RecordScript.of(expr).addArgument("$envl", range);
		
		Plan plan;
		plan = Plan.builder("test_range_query")
					.load(INPUT)
					.filter(predicate)
					.aggregate(AggregateFunction.COUNT())
					.build();
		return marmot.executeToLong(plan).get();
	}
	
	private static long countSequenceFile2(MarmotRuntime marmot, Envelope range) {
		Plan plan;
		plan = Plan.builder("test_range_query")
					.query(INPUT, range)
					.aggregate(AggregateFunction.COUNT())
					.build();
		return marmot.executeToLong(plan).get();
	}
	
	private static long countSpatialCluster(MarmotRuntime marmot, Envelope range) {
		Plan plan;
		
//		plan = Plan.builder("test_range_query")
//					.query(INPUT2, range)
//					.project("PRESENT_SN")
//					.build();
//		System.out.println();
//		marmot.executeToRecordSet(plan).fstream()
//					.map(r -> r.getString(0))
//					.forEach(System.out::println);
		
		plan = Plan.builder("test_range_query_cluster")
					.query(INPUT2, range)
					.aggregate(AggregateFunction.COUNT())
					.build();
		return marmot.executeToLong(plan).get();
	}
	
	private static long countSpatialIndex(MarmotRuntime marmot, Envelope range) {
		Plan plan;
		
//		plan = Plan.builder("test_range_query")
//					.query(INPUT3, range)
//					.project("PRESENT_SN")
//					.build();
//		System.out.println();
//		marmot.executeToRecordSet(plan).fstream()
//				.map(r -> r.getString(0))
//				.forEach(System.out::println);
		
		plan = Plan.builder("test_range_query_index")
					.query(INPUT3, range)
					.aggregate(AggregateFunction.COUNT())
					.build();
		return marmot.executeToLong(plan).get();
	}
}
