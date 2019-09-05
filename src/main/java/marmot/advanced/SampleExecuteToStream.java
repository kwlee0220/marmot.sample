package marmot.advanced;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.DefaultRecord;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExecuteToStream {
	private static final String TINY = "교통/지하철/서울역사";
	private static final String SMALL = "로그/나비콜/택시로그";
	private static final String MEDIUM = "건물/건물통합정보마스터/201809";
	private static final String LARGE = "교통/dtg_s";
	private static final String RANGE = "구역/행정동코드";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Envelope range = getRange(marmot);

		perfBatchTiny(marmot);
		perfStreamTiny(marmot);
		System.out.println("-------------------");
		perfBatchSmall(marmot, range);
		perfStreamSmall(marmot, range);
		System.out.println("-------------------");
		perfBatchMedium(marmot);
		perfStreamMedium(marmot);
		System.out.println("-------------------");
		perfBatchLarge(marmot);
		perfStreamLarge(marmot);
	}

	
	
	private static final void perfBatchTiny(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("perf_batch_tiny")
					.load(TINY)
					.filter("trnsit_yn == 1")
					.store(RESULT, FORCE)
					.build();
		
		StopWatch watch = StopWatch.start();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		try ( RecordSet rset = result.read() ) {
			Record output = DefaultRecord.of(rset.getRecordSchema());
			rset.next(output);
			String firstRecTime = watch.getElapsedMillisString();
			
			while ( rset.next(output) );
			String lastRecTime = watch.getElapsedMillisString();
			System.out.printf("elapsed (tiny,batch): first=%s last=%s%n", firstRecTime, lastRecTime);
		}
	}
	private static final void perfStreamTiny(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("perf_stream_tiny")
					.load(TINY)
					.filter("trnsit_yn == 1")
					.build();
		
		StopWatch watch = StopWatch.start();		
		try ( RecordSet rset = marmot.executeToStream("test", plan) ) {
			Record output = DefaultRecord.of(rset.getRecordSchema());
			rset.next(output);
			String firstRecTime = watch.getElapsedMillisString();
			
			while ( rset.next(output) );
			String lastRecTime = watch.getElapsedMillisString();
			System.out.printf("elapsed (tiny,stream): first=%s last=%s%n", firstRecTime, lastRecTime);
		}
	}
	

	
	private static final Envelope getRange(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("range")
					.load(RANGE)
					.filter("hdong_name == '행궁동'")
					.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
	private static final void perfBatchSmall(MarmotRuntime marmot, Envelope range) {
		Plan plan;
		plan = marmot.planBuilder("test batch_medium")
					.load(SMALL)
					.filterSpatially("the_geom", INTERSECTS, range)
					.store(RESULT, FORCE)
					.build();
		
		StopWatch watch = StopWatch.start();	
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		try ( RecordSet rset = result.read() ) {
			Record output = DefaultRecord.of(rset.getRecordSchema());
			rset.next(output);
			String firstRecTime = watch.getElapsedMillisString();
			
			while ( rset.next(output) );
			String lastRecTime = watch.getElapsedMillisString();
			System.out.printf("elapsed (small,batch): first=%s last=%s%n", firstRecTime, lastRecTime);
		}
	}
	private static final void perfStreamSmall(MarmotRuntime marmot, Envelope range) {
		Plan plan;
		plan = marmot.planBuilder("test SampleGetStream")
					.load(SMALL)
					.filterSpatially("the_geom", INTERSECTS, range)
					.build();
		
		StopWatch watch = StopWatch.start();		
		try ( RecordSet rset = marmot.executeToStream("test", plan) ) {
			Record output = DefaultRecord.of(rset.getRecordSchema());
			rset.next(output);
			String firstRecTime = watch.getElapsedMillisString();
			
			while ( rset.next(output) );
			String lastRecTime = watch.getElapsedMillisString();
			System.out.printf("elapsed (small,stream): first=%s last=%s%n", firstRecTime, lastRecTime);
		}
	}
	
	
	private static final void perfBatchMedium(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("perf_batch_medium")
					.load(MEDIUM)
					.filter("grnd_flr >= 20")
					.store(RESULT, FORCE)
					.build();
		
		StopWatch watch = StopWatch.start();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		try ( RecordSet rset = result.read() ) {
			Record output = DefaultRecord.of(rset.getRecordSchema());
			rset.next(output);
			String firstRecTime = watch.getElapsedMillisString();
			
			while ( rset.next(output) );
			String lastRecTime = watch.getElapsedMillisString();
			System.out.printf("elapsed (medium,batch): first=%s last=%s%n", firstRecTime, lastRecTime);
		}
	}
	private static final void perfStreamMedium(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("perf_stream_medium")
					.load(MEDIUM)
					.filter("grnd_flr >= 20")
					.build();
		
		StopWatch watch = StopWatch.start();		
		try ( RecordSet rset = marmot.executeToStream("test", plan) ) {
			Record output = DefaultRecord.of(rset.getRecordSchema());
			rset.next(output);
			String firstRecTime = watch.getElapsedMillisString();
			
			while ( rset.next(output) );
			String lastRecTime = watch.getElapsedMillisString();
			System.out.printf("elapsed (medium,stream): first=%s last=%s%n", firstRecTime, lastRecTime);
		}
	}
	
	
	private static final void perfBatchLarge(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("perf_batch_large")
					.load(LARGE)
					.filter("운행속도 == 1 && rpm > 1000")
					.store(RESULT, FORCE)
					.build();
		
		StopWatch watch = StopWatch.start();	
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		try ( RecordSet rset = result.read() ) {
			Record output = DefaultRecord.of(rset.getRecordSchema());
			rset.next(output);
			String firstRecTime = watch.getElapsedMillisString();
			
			while ( rset.next(output) );
			String lastRecTime = watch.getElapsedMillisString();
			System.out.printf("elapsed (large,batch): first=%s last=%s%n", firstRecTime, lastRecTime);
		}
	}
	private static final void perfStreamLarge(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("perf_stream_large")
					.load(LARGE)
					.filter("운행속도 == 1 && rpm > 1000")
					.build();
		
		StopWatch watch = StopWatch.start();		
		try ( RecordSet rset = marmot.executeToStream("test", plan) ) {
			Record output = DefaultRecord.of(rset.getRecordSchema());
			rset.next(output);
			String firstRecTime = watch.getElapsedMillisString();
			
			while ( rset.next(output) );
			String lastRecTime = watch.getElapsedMillisString();
			System.out.printf("elapsed (large,stream): first=%s last=%s%n", firstRecTime, lastRecTime);
		}
	}
	
}
