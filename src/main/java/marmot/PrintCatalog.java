package marmot;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PrintCatalog {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		System.out.println(marmot.getSubDirAll("/", false));
		
		// 카다로그에 등록된 모든 레이어 등록정보를 출력한다.
		List<DataSet> dsList = marmot.getDataSetAll();
		for ( DataSet ds: dsList ) {
			System.out.println("DataSet name: " + ds.getId());
			System.out.println("\tType: " + ds.getType());
			if ( ds.hasGeometryColumn() ) {
				GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

				System.out.println("\tGeometry column: " + gcInfo.name());
				System.out.println("\tSRID: " + gcInfo.srid());
			}
			
			System.out.println("\t# of records: " + ds.getRecordCount());

			System.out.println("\tColumns: ");
			RecordSchema schema = ds.getRecordSchema();
			for ( Column col: schema.getColumns() ) {
				System.out.printf("\t\t%02d:%s, %s%n", col.ordinal(), col.name(),
												col.type().getName());
			}
		}
		
		// 특정 이름의 레이어의 등록정보를 접근
		DataSet ds = marmot.getDataSet("교통/지하철/서울역사");
		
		// 카다로그에 등록된 모든 폴더를 접근한다.
		List<String> folders = marmot.getDirAll();
		for ( String folder: folders ) {
			System.out.println("Dir: " + folder);
			
			// 폴더에 저장된 레이어의 등록정보를 접근한다.
			List<DataSet> datasets = marmot.getDataSetAllInDir(folder, false);
			
			// 폴더에 등록된 하위 폴더를 접근한다.
			List<String> subDirs = marmot.getSubDirAll(folder, false);
		}
		
		// 특정 이름의 레이어를 삭제시킨다.
		marmot.deleteDataSet("tmp/result");
		
		// 특정 폴더에 등록된 모든 레이어들을 삭제시킨다. (모든 하위 폴더의 레이어들도 삭제 대상임)
		marmot.deleteDir("tmp");
	}
}
