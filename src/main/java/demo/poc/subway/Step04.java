package demo.poc.subway;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.analysis.module.NormalizeParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.Group;
import marmot.plan.LoadOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step04 {
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
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		System.out.println("시작: 서울지역 지하철 역사 후보지 추천:...... ");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet blocks = marmot.getDataSet(FindBestSubway.OUTPUT(ANALY_BLOCK_RATIO));
		
		// 격자별 택시 승하차 수 집계
		DataSet ds = gridTaxiLog(marmot, blocks, FindBestSubway.OUTPUT(ANALY_TAXI_LOG));;
		System.out.printf("종료: %s(%d건), 소요시간=%ss%n",
							ds.getId(), ds.getRecordCount(), watch.getElapsedMillisString());
	}

	static DataSet gridTaxiLog(MarmotRuntime marmot, DataSet blocks, String outDsId) {
		StopWatch watch = StopWatch.start();
		
		System.out.print("\t단계: 격자별 택시승하차 집계 후 정규화 -> ");
		String tempOutDsId = FindBestSubway.TEMP_OUTPUT(ANALY_TAXI_LOG);
		
		DataSet taxi = marmot.getDataSet(TAXI_LOG);
		GeometryColumnInfo gcInfo = taxi.getGeometryColumnInfo();
		
		String outJoinCols = String.format("left.*-{%s},right.*-{%s}", gcInfo.name(), blocks.getGeometryColumn());
		
		Plan plan;
		PlanBuilder builder = Plan.builder("격자별_택시승하차_집계")
									// 택시 로그를  읽는다.
									.load(TAXI_LOG, LoadOptions.FIXED_MAPPERS)
									
									// 승하차 로그만 선택한다.
									.filter("status == 1 || status == 2");
		if ( !gcInfo.srid().equals(blocks.getGeometryColumnInfo().srid()) ) {
			builder = builder.transformCrs(gcInfo.name(), gcInfo.srid(),
											blocks.getGeometryColumnInfo().srid());
		}
		plan = builder		
					// 로그에 격자 정보 부가함
					.spatialJoin(gcInfo.name(), blocks.getId(), outJoinCols)
	
					// 격자별로 택시 승하차 로그수를 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"), SUM("portion"))
					
					.store(tempOutDsId, FORCE)
					.build();
		marmot.execute(plan);

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(tempOutDsId);
		params.outputDataset(outDsId);
		params.inputFeatureColumns("sum");
		params.outputFeatureColumns("normalized");
		marmot.executeProcess("normalize", params.toMap());

		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%ss%n", ds.getId(), ds.getRecordCount(),
														watch.getElapsedMillisString());
		
		return ds;
	}
}
