package demo.poc.subway;

import utils.StopWatch;

import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step01 {
	private static final String SID = "구역/시도";
	private static final String STATIONS = "교통/지하철/역사";
	private static final String TAXI_LOG = "나비콜/택시로그";
	private static final String BLOCKS = "지오비전/집계구/2015";
	private static final String FLOW_POP_BYTIME = "지오비전/유동인구/%d/월별_시간대";
	private static final String CARD_BYTIME = "지오비전/카드매출/%d/일별_시간대";
	private static final String RESULT = "분석결과/지하철역사_추천";
//	private static final int[] YEARS = new int[] {2015, 2016, 2017};
	private static final int[] YEARS = new int[] {2015};

	private static final String ANALY = "지하철역사_추천";
	private static final String ANALY_SEOUL = "지하철역사_추천/서울특별시_영역";
	private static final String ANAL_STATIONS = "지하철역사_추천/서울지역_지하철역사_버퍼";
	private static final String ANALY_BLOCK_RATIO = "지하철역사_추천/격자_집계구_겹침_비율";
	private static final String ANALY_TAXI_LOG = "지하철역사_추천/격자별_택시승하차";
	private static final String ANALY_FLOW_POP = "지하철역사_추천/격자별_유동인구";
	private static final String ANALY_CARD = "지하철역사_추천/격자별_카드매출";
	private static final String ANALY_MERGE = "지하철역사_추천/통합";
	
	public static final void main(String... args) throws Exception {
		System.out.println("시작: 서울지역 지하철 역사 후보지 추천:...... ");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		// 서울지역 지하철 역사의 버퍼 작업을 수행한다.
		DataSet ds = FindBestSubway.bufferSubway(marmot, STATIONS, FindBestSubway.OUTPUT(ANAL_STATIONS));
		
		System.out.printf("종료: %s(%d건), 소요시간=%ss%n",
							ds.getId(), ds.getRecordCount(), watch.getElapsedMillisString());
	}
}
