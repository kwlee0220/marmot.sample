package marmot;

import java.io.File;
import java.nio.charset.Charset;

import marmot.command.MarmotClientCommands;
import marmot.externio.shp.ShapefileRecordSet;
import marmot.optor.CreateDataSetOptions;
import marmot.optor.StoreDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExecutePlanLocally {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		sample1(marmot);
		sample2(marmot);
		sample3(marmot);
		sample4(marmot);
		sample5(marmot);
	}
	
	private static void sample1(PBMarmotClient marmot) {
		Plan plan = Plan.builder("test")
							.load(INPUT)
							.filter("sub_sta_sn > 300 && sub_sta_sn < 305")
							.project("sub_sta_sn")
							.build();
		try ( RecordSet rset = marmot.executeLocally(plan) ) {
			rset.forEach(System.out::println);
		}
	}
	
	private static void sample2(PBMarmotClient marmot) {
		Plan plan = Plan.builder("test")
							.load(INPUT)
							.filter("sub_sta_sn > 300 && sub_sta_sn < 305")
							.project("sub_sta_sn")
							.store(RESULT)
							.build();
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		marmot.createDataSet(RESULT, schema, CreateDataSetOptions.FORCE);
		try ( RecordSet rset = marmot.executeLocally(plan) ) {
			rset.forEach(System.out::println);
		}
		marmot.getDataSet(RESULT).read().forEach(System.out::println);
	}

	private static final File SHP_FILE = new File("/mnt/data/sbdata/data/포스웨이브/서울지하철역사");
	private static void sample3(PBMarmotClient marmot) {
		try ( ShapefileRecordSet rset = new ShapefileRecordSet(SHP_FILE, Charset.forName("euc-kr")) ) {
			Plan plan = Plan.builder("test")
								.filter("sub_sta_sn > 300 && sub_sta_sn < 305")
								.project("sub_sta_sn")
								.store(RESULT, StoreDataSetOptions.FORCE)
								.build();
			marmot.executeLocally(plan, rset).forEach(System.out::println);
		}
	}
	
	private static void sample4(PBMarmotClient marmot) {
		try ( ShapefileRecordSet rset = new ShapefileRecordSet(SHP_FILE, Charset.forName("euc-kr")) ) {
			Plan plan = Plan.builder("test")
								.filter("sub_sta_sn > 300 && sub_sta_sn < 305")
								.project("sub_sta_sn")
								.build();
			RecordSchema schema = marmot.getOutputRecordSchema(plan, rset.getRecordSchema());
			marmot.createDataSet(RESULT, schema, CreateDataSetOptions.FORCE);
			try ( RecordSet result = marmot.executeLocally(plan, rset) ) {
				Record r = DefaultRecord.of(result.getRecordSchema());
				while ( result.next(r) ) {
					System.out.println(r);
				}
			}
			marmot.getDataSet(RESULT).read().forEach(System.out::println);
		}
	}

	private static final File SHP_FILE2 = new File("/mnt/data/sbdata/data/사업단자료/행정동코드");
	private static void sample5(PBMarmotClient marmot) {
		try ( ShapefileRecordSet rset = new ShapefileRecordSet(SHP_FILE2, Charset.forName("utf-8")) ) {
			Plan plan = Plan.builder("test")
								.project("the_geom,hcode")
								.build();
			RecordSchema schema = marmot.getOutputRecordSchema(plan, rset.getRecordSchema());
			marmot.createDataSet(RESULT, schema, CreateDataSetOptions.FORCE);
			try ( RecordSet result = marmot.executeLocally(plan, rset) ) {
				System.out.println("count = " + result.count());
			}
		}
	}
}
