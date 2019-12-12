package marmot;

import static marmot.optor.CreateDataSetOptions.FORCE;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Lists;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.GeoClientUtils;
import marmot.remote.protobuf.PBMarmotClient;
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
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
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
		System.out.printf("block_size=%d%s%n",
							ds.getBlockSize(),
							ds.getCompressionCodecName()
								.map(name -> ", compression=" + name)
								.getOrElse(""));
		
		ds = marmot.createDataSet("tmp/result", schema, FORCE.blockSize(64)
															.compressionCodecName("snappy"));
		System.out.printf("block_size=%d(=64)%s%n",
							ds.getBlockSize(),
							ds.getCompressionCodecName()
								.map(name -> ", compression=" + name)
								.getOrElse(""));
		
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
		RecordSet rset = RecordSet.from(schema, recordList);

		// 데이터세트를 생성하고, 레코드 세트를 저장한다.
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		ds = marmot.createDataSet("tmp/result2", schema, FORCE(gcInfo));
		System.out.printf("geometry=%s, block_size=%d%s%n",
							ds.getGeometryColumnInfo(), ds.getBlockSize(),
							ds.getCompressionCodecName()
								.map(name -> ", compression=" + name)
								.getOrElse(""));
		ds.append(rset);
		
		ds = marmot.getDataSet("tmp/result2");
		SampleUtils.printPrefix(ds, 5);

		/*
		SpatialIndexInfo idxInfo;
		
		idxInfo = ds.cluster();
		System.out.printf("created index: ds=%s, geom=%s, tile_mbr=%s%n",
							idxInfo.getDataSetId(), idxInfo.getGeometryColumnInfo(),
							idxInfo.getTileBounds());
		
		idxInfo = ds.getDefaultSpatialIndexInfo().get();
		
		ds.deleteSpatialCluster();
		Preconditions.checkState(!ds.isSpatiallyClustered());
		System.out.printf("deleted spatial cluster: ds=%s%n", ds.getId());
		
		idxInfo = ds.cluster();
		System.out.printf("created index again: ds=%s, geom=%s, tile_mbr=%s%n",
							idxInfo.getDataSetId(), idxInfo.getGeometryColumnInfo(),
							idxInfo.getTileBounds());
		
		idxInfo = ds.getDefaultSpatialIndexInfo().get();
		
		ds.deleteSpatialCluster();
		Preconditions.checkState(!ds.isSpatiallyClustered());
		System.out.printf("deleted spatial cluster again: ds=%s%n", ds.getId());
*/
	}
}
