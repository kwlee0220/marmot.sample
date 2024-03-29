package marmot;

import utils.StopWatch;

import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleMarmotClient {
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		marmot.deleteDataSet("tmp/result");

		Plan plan;
		DataSet ds;
	
		// 데이터세트를 읽어 화면에 출력
		//
//		ds = marmot.getDataSet("교통/지하철/서울역사");
		ds = marmot.getDataSet("주소/건물POI");
		try ( RecordSet rset = ds.read() ) {
			rset.fstream().take(5).forEach(System.out::println);
//			rset.stream().limit(50000000).count();
		}
		
		RecordSet rset;
		
		// Plan을 이용한 데이터 접근
		//
		plan = Plan.builder("test")
					.load("교통/지하철/서울역사")
					.filter("kor_sub_nm.length() > 5")
					.project("sub_sta_sn,kor_sub_nm")
					.build();
//		rset = marmot.executeLocally(plan);
//		SampleUtils.printPrefix(rset, 5);
		
		// 사용자가 제공하는 입력 레코드세트를 활용한 Plan 수행.
		//
		ds = marmot.getDataSet("교통/지하철/서울역사");
		plan = Plan.builder("test2")
					.filter("kor_sub_nm.length() > 5")
					.project("sub_sta_sn,kor_sub_nm")
					.build();
		
//		try ( RecordSet input = ds.read() ) {
//			try ( RecordSet result = marmot.executeLocally(plan, input) ) {
//				SampleUtils.printPrefix(result, 5);
//			}
//		}
		
		marmot.close();
	}
}
