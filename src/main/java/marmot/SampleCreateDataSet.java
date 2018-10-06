package marmot;

import static marmot.DataSetOption.BLOCK_SIZE;
import static marmot.DataSetOption.COMPRESS;
import static marmot.DataSetOption.FORCE;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Lists;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.command.MarmotClient;
import marmot.geo.GeoClientUtils;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.rset.RecordSets;
import marmot.support.DefaultRecord;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleCreateDataSet {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();
		
		// 생성될 데이터세트의 스키마를 정의함.
		RecordSchema schema = RecordSchema.builder()
											.addColumn("the_geom", DataType.POINT)
											.addColumn("name", DataType.STRING)
											.addColumn("age", DataType.INT)
											.addColumn("gpa", DataType.DOUBLE)
											.build();
		
		// 생성할 데이터세트에 저장될 레코드를 담을 레코드 객체를 생성
		Record record = DefaultRecord.of(schema);
		
		DataSet ds;
		ds = marmot.createDataSet("tmp/result", schema, FORCE);
		System.out.printf("block_size=%d, compression=%s(=false)%n", ds.getBlockSize(), ds.getCompression());
		
		ds = marmot.createDataSet("tmp/result", schema, BLOCK_SIZE(64), COMPRESS, FORCE);
		System.out.printf("block_size=%d(=64), compression=%s(=true)%n", ds.getBlockSize(), ds.getCompression());
		
		// 생성할 데이터세트에 저장될 레코드들의 리스트를 생성.
		List<Record> recordList = Lists.newArrayList();
		
		// 첫번째 레코드 (컬럼 이름을 통한 설정 )
		record.set("the_geom", GeoClientUtils.toPoint(1.0, 2.0));
		record.set("name", "홍길동");
		record.set("age", 15);
		record.set("gpa", 2.8);
		recordList.add(record.duplicate());

		// 두번째 레코드 (컬럼 번호를 통한 설정)
		record.clear();
		record.set(0, GeoClientUtils.toPoint(7.0, 9.5));
		record.set(1, "성춘향");
		record.set(2, 18);
		record.set(3, 2.5);
		recordList.add(record.duplicate());

		// 세번째 레코드 (컬럼 값 리스트를 통한 설정)
		record.clear();
		record.setAll(Arrays.asList(GeoClientUtils.toPoint(7.0, 9.5), "이몽룡", 19, 3.1));
		recordList.add(record.duplicate());
		
		// 생성된 레코드들을 이용하여 레코드 세트 생성
		RecordSet rset = RecordSets.from(schema, recordList);

		// 데이터세트를 생성하고, 레코드 세트를 저장한다.
		ds = marmot.createDataSet("tmp/test", rset.getRecordSchema(), FORCE)
					.append(rset);
		
		SampleUtils.printPrefix(ds, 5);
	}
}