package demo.poc.subway;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.AggregateFunction.UNION_GEOM;
import static marmot.optor.JoinOptions.FULL_OUTER_JOIN;
import static marmot.optor.JoinOptions.INNER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.google.common.collect.Lists;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.analysis.module.NormalizeParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import marmot.plan.LoadOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindBestSubway {
	private static final String SID = "구역/시도";
	private static final String STATIONS = "교통/지하철/역사";
	private static final String TAXI_LOG = "나비콜/택시로그";
	private static final String BLOCKS = "지오비전/집계구/2015";
	private static final String FLOW_POP_BYTIME = "지오비전/유동인구/%d/월별_시간대";
	private static final String CARD_BYTIME = "지오비전/카드매출/%d/일별_시간대";
	private static final String RESULT = "분석결과/지하철역사_추천";
	private static final int[] YEARS = new int[] {2015, 2016, 2017};
//	private static final int[] YEARS = new int[] {2015};

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
		
		// 서울지역 지하철 역사의 버퍼 작업을 수행한다.
		DataSet subway = bufferSubway(marmot, OUTPUT(ANAL_STATIONS));
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		DataSet seoul = getSeoulBoundary(marmot, OUTPUT(ANALY_SEOUL));
		
		// 격자별 집계구 비율 계산
		DataSet blocks = calcBlockRatio(marmot, seoul, subway, OUTPUT(ANALY_BLOCK_RATIO));
		
		// 격자별 택시 승하차 수 집계
		DataSet taxi = gridTaxiLog(marmot, blocks, OUTPUT(ANALY_TAXI_LOG));
		
		// 격자별 유동인구 집계
		DataSet flowPop = gridFlowPopulation(marmot, blocks, OUTPUT(ANALY_FLOW_POP));
		
		// 격자별 카드매출 집계
		DataSet cards = gridCardSales(marmot, blocks, OUTPUT(ANALY_CARD));
		
		// 격자별_유동인구_카드매출_택시승하차_비율 합계
		DataSet result = mergeAll(marmot, taxi, flowPop, cards, OUTPUT(ANALY_MERGE));
		
		// 공간객체 부여
		attachGeom(marmot, result, blocks, RESULT);
		
		marmot.deleteDir(OUTPUT(ANALY));
		
		DataSet ds = marmot.getDataSet(RESULT);
		System.out.printf("종료: %s(%d건), 소요시간=%ss%n",
							ds.getId(), ds.getRecordCount(), watch.getElapsedMillisString());
	}

	private static DataSet bufferSubway(MarmotRuntime marmot, String outDsId) {
		System.out.print("\t단계: '전국_지하쳘_역사' 중 서울 소재 역사 추출 후 1KM 버퍼 계산 -> ");
		
		StopWatch watch = StopWatch.start();
		
		// 서울지역 지하철 역사를 구하고 1km 버퍼를 구한다.
		DataSet stations = marmot.getDataSet(STATIONS);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();

		Plan plan;
		plan = Plan.builder("서울지역 지하철역사 1KM 버퍼")
					.load(STATIONS)
					.filter("sig_cd.substring(0,2) == '11'")
					.buffer(gcInfo.name(), 1000)
					.project("the_geom")
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);

		// 서울지역 지하철 역사에 대한 공간색인을 생성한다.
		DataSet ds = marmot.getDataSet(outDsId);
		ds.cluster();
		
		System.out.printf("%s(%d건), 소요시간=%ss%n",
							outDsId, ds.getRecordCount(), watch.getElapsedMillisString());
		
		return ds;
	}
	
	private static DataSet getSeoulBoundary(MarmotRuntime marmot, String outDsId) {
		StopWatch watch = StopWatch.start();

		System.out.print("\t단계: '전국_법정시도경계'에서 서울특별시 영역 추출 -> ");
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(SID).getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		// 서울지역 버퍼에 대한 공간색인을 생성한다.
		DataSet ds = marmot.getDataSet(outDsId);
		ds.cluster();
		
		System.out.printf("1건, 소요시간=%ss%n", watch.getElapsedMillisString());
		return marmot.getDataSet(outDsId);
	}
	
	private static DataSet calcBlockRatio(MarmotRuntime marmot, DataSet seoul, DataSet subway,
										String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.print("\t단계: '지오비전_집계구'와 격자 겹치는 영역 비율 계산 -> ");
		
		DataSet stations = marmot.getDataSet(BLOCKS);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();
		SquareGrid grid = new SquareGrid(seoul.getId(), new Size2d(300, 300));
		
		Plan plan;
		plan = Plan.builder("격자_집계구_비율")
					.load(BLOCKS)
					.filter("block_cd.substring(0,2) == '11'")
					.differenceJoin(gcInfo.name(), subway.getId())
					.assignGridCell(gcInfo.name(), grid, false)
					.intersection(gcInfo.name(), "cell_geom", "overlap")
					.defineColumn("portion:double", "portion = ST_Area(overlap) / ST_Area(cell_geom)")
					.project("overlap as the_geom, cell_id, cell_pos, block_cd, portion")
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		// 격자_집계구_비율에 대한 공간색인을 생성한다.
		DataSet ds = marmot.getDataSet(outDsId);
		ds.cluster();

		System.out.printf("%s(%d건), 소요시간=%ss%n",
							outDsId, ds.getRecordCount(), watch.getElapsedMillisString());
		
		return ds;
	}

	private static DataSet gridTaxiLog(MarmotRuntime marmot, DataSet blocks, String outDsId) {
		StopWatch watch = StopWatch.start();
		
		System.out.print("\t단계: 격자별 택시승하차 집계 후 정규화 -> ");
		String tempOutDsId = TEMP_OUTPUT(ANALY_TAXI_LOG);
		
		DataSet taxi = marmot.getDataSet(TAXI_LOG);
		GeometryColumnInfo gcInfo = taxi.getGeometryColumnInfo();
		String outJoinCols = String.format("*-{%s},param.*-{%s}", gcInfo.name(), blocks.getGeometryColumn());
		
		Plan plan;
		plan = Plan.builder("격자별_택시승하차_집계")
					// 택시 로그를  읽는다.
					.load(TAXI_LOG)
					
					// 승하차 로그만 선택한다.
					.filter("status == 1 || status == 2")
					
					// 로그에 격자 정보 부가함
					.spatialJoin(gcInfo.name(), blocks.getId(), outJoinCols)
	
					// 격자별로 택시 승하차 로그수를 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"), COUNT())
					
					.store(tempOutDsId, FORCE)
					.build();
		marmot.execute(plan);

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(tempOutDsId);
		params.outputDataset(outDsId);
		params.inputFeatureColumns("count");
		params.outputFeatureColumns("normalized");
		marmot.executeProcess("normalize", params.toMap());

		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%ss%n", ds.getId(), ds.getRecordCount(),
														watch.getElapsedMillisString());
		
		return ds;
	}

	private static DataSet gridFlowPopulation(MarmotRuntime marmot, DataSet blocks,
											String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.printf(String.format("\t단계: 연차별로 격자단위 유동인구 수집 -> %n"));
		
		List<String> DS_IDS = Lists.newArrayList();
		for ( int year: YEARS ) {
			gridFlowPopulation(marmot, year, blocks, OUTPUT(ANALY_FLOW_POP, year));
			DS_IDS.add(OUTPUT(ANALY_FLOW_POP, year));
		}
		
		Plan plan;
		plan = Plan.builder("격자별_유동인구_통합")
					.load(DS_IDS, LoadOptions.DEFAULT)
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
										AVG("normalized").as("normalized"))
					.store(OUTPUT(ANALY_FLOW_POP), FORCE)
					.build();
		marmot.execute(plan);
		
		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("\t\t%s(%d건), 소요시간=%s%n",
							ds.getId(), ds.getRecordCount(), watch.getElapsedMillisString());
		
		return ds;
	}
	
	private static void gridFlowPopulation(MarmotRuntime marmot, int year, DataSet blocks,
											String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.printf(String.format("\t\t단계: %d년도 격자별 유동인구수 정규화 -> ", year));
		
		String planName = String.format("%d년도_격자별_유동인구_수집", year);
		String inDsId = String.format(FLOW_POP_BYTIME, year);
		String tempOutDsId = outDsId + "_집계";

		String sumExpr = IntStream.range(0, 24)
									.mapToObj(idx -> String.format("avg_%02dtmst", idx))
									.collect(Collectors.joining("+"));
		String avgExpr = String.format("(%s) / 24", sumExpr);

		Plan plan;
		plan = Plan.builder(planName)
					// 유동인구를  읽는다.
					.load(inDsId)
					
					// 서울지역 데이터만 선택
					.filter("block_cd.startsWith('11')")
					
					// 하루동안의 시간대별 평균 유동인구를 계산
					.defineColumn("daily_avg:double", avgExpr)
					
					// 집계구별 평균 일간 유동인구 평균 계산
					.aggregateByGroup(Group.ofKeys("block_cd").workerCount(7), AVG("daily_avg"))

					// 격자 정보 부가함
					.hashJoin("block_cd", blocks.getId(), "block_cd", "param.*,avg", INNER_JOIN)
					
					// 비율 값 반영
					.update("avg *= portion")
					
					// 격자별 평균 유동인구을 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"), SUM("avg").as("avg"))
						
					.store(tempOutDsId, FORCE)
					.build();
		marmot.execute(plan);

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(tempOutDsId);
		params.outputDataset(outDsId);
		params.inputFeatureColumns("avg");
		params.outputFeatureColumns("normalized");
		marmot.executeProcess("normalize", params.toMap());
		
		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%s%n", ds.getId(), ds.getRecordCount(),
														watch.getElapsedMillisString());
	}

	private static DataSet gridCardSales(MarmotRuntime marmot, DataSet blocks, String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.printf(String.format("\t단계: 연차별로 격자단위 카드매출액 수집 -> %n"));
		
		List<String> DS_IDS = Lists.newArrayList();
		for ( int year: YEARS ) {
			gridCardSales(marmot, year, blocks, OUTPUT(ANALY_CARD, year));
			DS_IDS.add(OUTPUT(ANALY_CARD, year));
		}
		
		Plan plan;
		plan = Plan.builder("격자별_카드매출_통합")
					.load(DS_IDS, LoadOptions.DEFAULT)
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
										AVG("normalized").as("normalized"))
					.store(OUTPUT(ANALY_CARD), FORCE)
					.build();
		marmot.execute(plan);
		
		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("\t\t%s(%d건), 소요시간=%s%n",
							ds.getId(), ds.getRecordCount(), watch.getElapsedMillisString());
		
		return ds;
	}
	
	private static void gridCardSales(MarmotRuntime marmot, int year, DataSet blocks,
										String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.printf(String.format("\t\t단계: %d년도 격자별 카드매출액 정규화 -> ", year));
		
		String planName = String.format("%d년도 격자별_카드매출 집계", year);
		String inDsId = String.format(CARD_BYTIME, year);
		String tempOutDsId = outDsId + "_집계";
		
		String sumExpr = IntStream.range(0, 24)
									.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
									.collect(Collectors.joining("+"));

		Plan plan;
		plan = Plan.builder(planName)
					// 카드매출을  읽는다.
					.load(inDsId)
					
					// 서울지역 데이터만 선택
					.filter("block_cd.startsWith('11')")
					
					// 하루동안의 카드매출 합계를 계산
					.defineColumn("amount:double", sumExpr)
					
					// 집계구별 일간 카드매출 합계 계산
					.aggregateByGroup(Group.ofKeys("block_cd").workerCount(11),
										AVG("amount").as("amount"))

					// 격자 정보 부가함
					.hashJoin("block_cd", blocks.getId(), "block_cd", "param.*,amount", INNER_JOIN)

					// 비율 값 반영
					.update("amount *= portion")
					
					// 격자별 평균 카드매출액을 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"), SUM("amount").as("amount"))
						
					.store(tempOutDsId, FORCE)
					.build();
		marmot.execute(plan);

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(tempOutDsId);
		params.outputDataset(outDsId);
		params.inputFeatureColumns("amount");
		params.outputFeatureColumns("normalized");
		marmot.executeProcess("normalize", params.toMap());
		
		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%s%n", ds.getId(), ds.getRecordCount(),
														watch.getElapsedMillisString());
	}
	
	private static DataSet mergeAll(MarmotRuntime marmot, DataSet taxi, DataSet flowPop,
									DataSet card, String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.printf(String.format("\t단계: 격자별_유동인구_카드매출_택시승하차_표준값_합계 -> "));
		
		String merge1 = "if ( normalized == null ) {"
					+ "		cell_id = right_cell_id;"
					+ "		normalized = 0;"
					+ "} else if ( right_normalized == null ) {"
					+ "		right_normalized = 0;"
					+ "}"
					+ "normalized = normalized + right_normalized;";
		String merge2 = "if ( normalized == null ) {"
					+ "		cell_id = param_cell_id;"
					+ "		normalized = 0;"
					+ "} else if ( param_normalized == null ) {"
					+ "		param_normalized = 0;"
					+ "}"
					+ "normalized = normalized + param_normalized;";
		
		Plan plan;
		plan = Plan.builder("격자별_유동인구_카드매출_택시승하차_표준값_합계")
					.loadHashJoin(card.getId(), "cell_id", flowPop.getId(), "cell_id",
									"left.*,right.{cell_id as right_cell_id,"
												+ "normalized as right_normalized}",
									FULL_OUTER_JOIN(11))
					.update(merge1)
					.project("cell_id,normalized")
					
					.hashJoin("cell_id", taxi.getId(), "cell_id",
							"*,param.{cell_id as param_cell_id,"
									+ "normalized as param_normalized}", FULL_OUTER_JOIN(11))
					.update(merge2)
					.project("cell_id,normalized as value")
					
					.store(outDsId, FORCE)
					.build();
		marmot.execute(plan);
		
		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%s%n", ds.getId(), ds.getRecordCount(),
														watch.getElapsedMillisString());
		
		return ds;
	}
	
	private static void attachGeom(MarmotRuntime marmot, DataSet merged, DataSet blocks,
									String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.printf(String.format("\t단계: 격자별_결과에_공간객체_부여 -> "));
		
		GeometryColumnInfo gcInfo = blocks.getGeometryColumnInfo();
		String outJoinCols = String.format("%s,cell_id,cell_pos,param.value", gcInfo.name());
		
		Plan plan;
		plan = Plan.builder("공간데이터 첨부")
					.load(blocks.getId())
					
					// 격자별로 집계구 겹침 영역 union
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
										UNION_GEOM(gcInfo.name()))
					
					// 격자별로 결과 데이터 병합
					.hashJoin("cell_id", merged.getId(), "cell_id", outJoinCols, INNER_JOIN)
					
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%s%n", ds.getId(), ds.getRecordCount(),
														watch.getElapsedMillisString());
	}
	
	private static String OUTPUT(String analId) {
		return "/tmp/" + analId;
	}
	
	private static String OUTPUT(String analId, int year) {
		return "/tmp/" + analId + "_연도별/" + year;
	}
	
	private static String TEMP_OUTPUT(String analId) {
		return "/tmp/" + analId + "_집계";
	}
}
