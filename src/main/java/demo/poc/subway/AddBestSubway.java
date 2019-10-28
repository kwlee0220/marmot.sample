package demo.poc.subway;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.AggregateFunction.UNION_GEOM;
import static marmot.optor.JoinOptions.FULL_OUTER_JOIN;
import static marmot.optor.JoinOptions.INNER_JOIN;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.google.common.collect.Lists;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.exec.CompositeAnalysis;
import marmot.exec.ModuleAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.exec.SystemAnalysis;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import marmot.plan.LoadOptions;
import marmot.process.NormalizeParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Size2d;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AddBestSubway {
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
	private static final String ANAL_SUBWAY = "지하철역사_추천/서울지역_지하철역사_버퍼";
	private static final String ANALY_BLOCK_RATIO = "지하철역사_추천/집계구_비율";
	private static final String ANALY_TAXI_LOG = "지하철역사_추천/격자별_택시승하차";
	private static final String ANALY_FLOW_POP = "지하철역사_추천/격자별_유동인구";
	private static final String ANALY_CARD = "지하철역사_추천/격자별_카드매출";
	private static final String ANALY_MERGE = "지하철역사_추천/분야별_격자데이터_통합";
	private static final String ANALY_ATTACH_GEOM = "지하철역사_추천/공간데이터_병합";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		marmot.deleteAnalysis("지하철역사_추천", true);
	
		List<String> compIdList = new ArrayList<>();
		
		// 서울지역 지하철 역사의 버퍼 작업을 수행한다.
		createSubwayBuffer(marmot, compIdList);
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		createSeoulBoundary(marmot, compIdList);
		
		// 격자별 집계구 비율 계산
		calcBlockRatio(marmot, compIdList);
		
		// 격자별 택시 승하차 수 집계
		gridTaxiLog(marmot, compIdList);
		
		// 격자별 유동인구 집계
		gridFlowPopulation(marmot, compIdList);
		
		// 격자별 카드매출 집계
		gridCardSales(marmot, compIdList);
		
		// 격자별_유동인구_카드매출_택시승하차_비율 합계
		mergeAll(marmot, compIdList);

		// 공간객체 부여
		attachGeom(marmot, compIdList);
		
		SystemAnalysis delDir = SystemAnalysis.deleteDir(ANALY_TAXI_LOG + "_임시파일_삭제",
														OUTPUT(ANALY));
		marmot.addAnalysis(delDir, true);
		compIdList.add(delDir.getId());

		marmot.addAnalysis(new CompositeAnalysis("지하철역사_추천", compIdList), true);
	}

	private static void createSubwayBuffer(MarmotRuntime marmot, List<String> compIdList) {
		String analId = ANAL_SUBWAY;
		String outDsId = OUTPUT(ANAL_SUBWAY);
		
		DataSet stations = marmot.getDataSet(STATIONS);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();
		
		Plan plan;
		
		// 서울지역 지하철 역사를 구하고 1km 버퍼를 구한다.
		plan = marmot.planBuilder("서울지역_지하철역사_버퍼")
					.load(STATIONS)
					.filter("sig_cd.substring(0,2) == '11'")
					.buffer(gcInfo.name(), 1000)
					.project("the_geom")
					.store(outDsId, FORCE(gcInfo))
					.build();
		PlanAnalysis anal = new PlanAnalysis(analId, plan);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());

		// 서울지역 지하철 역사에 대한 공간색인을 생성한다.
		SystemAnalysis anal2 = SystemAnalysis.clusterDataSet(analId + "_색인", outDsId);
		marmot.addAnalysis(anal2, true);
		compIdList.add(anal2.getId());
	}
	
	private static void createSeoulBoundary(MarmotRuntime marmot, List<String> compIdList) {
		String analyId = ANALY_SEOUL;
		String outDsId = "/tmp/" + analyId;
		
		DataSet stations = marmot.getDataSet(SID);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();

		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		Plan plan;
		plan = marmot.planBuilder("서울특별시_영역_추출")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.store(outDsId, FORCE(gcInfo))
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(analyId, plan);
		marmot.addAnalysis(anal1, true);
		compIdList.add(anal1.getId());
		
		// 서울지역 버퍼에 대한 공간색인을 생성한다.
		SystemAnalysis anal2 = SystemAnalysis.clusterDataSet(analyId + "_색인", outDsId);
		marmot.addAnalysis(anal2, true);
		compIdList.add(anal2.getId());
	}
	
	private static void calcBlockRatio(MarmotRuntime marmot, List<String> compIdList) {
		String analyId = ANALY_BLOCK_RATIO;
		String outDsId = OUTPUT(ANALY_BLOCK_RATIO);
		
		DataSet blocks = marmot.getDataSet(BLOCKS);
		GeometryColumnInfo gcInfo = blocks.getGeometryColumnInfo();
		
		SquareGrid grid = new SquareGrid(OUTPUT(ANALY_SEOUL), new Size2d(300, 300));
		
		Plan plan;
		plan = marmot.planBuilder("격자별_집계구_비율")
					.load(BLOCKS)
					.filter("block_cd.substring(0,2) == '11'")
					.differenceJoin(gcInfo.name(), OUTPUT(ANAL_SUBWAY))
					.assignGridCell(gcInfo.name(), grid, false)
					.intersection(gcInfo.name(), "cell_geom", "overlap")
					.defineColumn("portion:double", "portion = ST_Area(overlap) / ST_Area(cell_geom)")
					.project("overlap as the_geom, cell_id, cell_pos, block_cd, portion")
					.store(outDsId, FORCE(gcInfo))
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(analyId, plan);
		marmot.addAnalysis(anal1, true);
		compIdList.add(anal1.getId());
		
		// 격자별_집계구_비율에 대한 공간색인을 생성한다.
		SystemAnalysis anal2 = SystemAnalysis.clusterDataSet(analyId + "_색인", outDsId);
		marmot.addAnalysis(anal2, true);
		compIdList.add(anal2.getId());
	}

	private static void gridTaxiLog(MarmotRuntime marmot, List<String> compIdList) {
		String analyId = ANALY_TAXI_LOG;
		String outDsId = OUTPUT(ANALY_TAXI_LOG);
		String tempOutDsId = TEMP_OUTPUT(ANALY_TAXI_LOG);
		
		List<String> subCompIdList = Lists.newArrayList();
		
		Plan plan;
		plan = marmot.planBuilder("격자별_택시승하차_집계")
					// 택시 로그를  읽는다.
					.load(TAXI_LOG)
					
					// 승하차 로그만 선택한다.
					.filter("status == 1 || status == 2")
					
					// 로그에 격자 정보 부가함
					.spatialJoin("the_geom", OUTPUT(ANALY_BLOCK_RATIO), "param.*")

					// 격자별로 택시 승하차 로그수를 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"), COUNT())
					
					.store(tempOutDsId, FORCE)
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(analyId + "/집계", plan);
		marmot.addAnalysis(anal1, true);
		subCompIdList.add(anal1.getId());

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(tempOutDsId);
		params.outputDataset(outDsId);
		params.inputFeatureColumns("count");
		params.outputFeatureColumns("normalized");
		ModuleAnalysis anal2 = new ModuleAnalysis(analyId + "/표준화", "normalize", params.toMap());
		marmot.addAnalysis(anal2, true);
		subCompIdList.add(anal2.getId());
		
		CompositeAnalysis anal = new CompositeAnalysis(analyId, subCompIdList);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());
	}

	private static void gridFlowPopulation(MarmotRuntime marmot, List<String> compIdList) {
		List<String> subCompIdList = Lists.newArrayList();
		
		List<String> DS_IDS = Lists.newArrayList();
		for ( int year: YEARS ) {
			gridFlowPopulation(marmot, year, subCompIdList);
			DS_IDS.add(OUTPUT(ANALY_FLOW_POP, year));
		}
		
		Plan plan;
		plan = marmot.planBuilder("격자별_유동인구_통합")
					.load(DS_IDS, LoadOptions.DEFAULT)
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
										AVG("normalized").as("normalized"))
					.update("normalized = normalized / " + YEARS.length)
					.store(OUTPUT(ANALY_FLOW_POP), FORCE)
					.build();
		PlanAnalysis panal = new PlanAnalysis(ANALY_FLOW_POP + "/년도별_표준값_병합", plan);
		marmot.addAnalysis(panal, true);
		subCompIdList.add(panal.getId());
		
//		SystemAnalysis sanal = SystemAnalysis.deleteDataSet(ANALY_FLOW_POP + "/년도별_표준값_삭제", DS_IDS);
//		marmot.addAnalysis(sanal, true);
//		subCompIdList.add(sanal.getId());
		
		CompositeAnalysis anal = new CompositeAnalysis(ANALY_FLOW_POP, subCompIdList);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());
	}
	
	private static void gridFlowPopulation(MarmotRuntime marmot, int year, List<String> compIdList) {
		String analyId = ANALY_FLOW_POP + "/" + year;
		String inDsId = String.format(FLOW_POP_BYTIME, year);
		String outDsId = OUTPUT(ANALY_FLOW_POP, year);
		String tempOutDsId = TEMP_OUTPUT(ANALY_FLOW_POP, year);
		
		List<String> subCompos = Lists.newArrayList();

		String sumExpr = IntStream.range(0, 24)
									.mapToObj(idx -> String.format("avg_%02dtmst", idx))
									.collect(Collectors.joining("+"));
		String avgExpr = String.format("(%s) / 24", sumExpr);

		Plan plan;
		String planName = String.format("%d년도_격자별_유동인구_수집", year);
		plan = marmot.planBuilder(planName)
					// 유동인구를  읽는다.
					.load(inDsId)
					
					// 서울지역 데이터만 선택
					.filter("block_cd.startsWith('11')")
					
					// 하루동안의 시간대별 평균 유동인구를 계산
					.defineColumn("daily_avg:double", avgExpr)
					
					// 집계구별 평균 일간 유동인구 평균 계산
					.aggregateByGroup(Group.ofKeys("block_cd").workerCount(7), AVG("daily_avg"))

					// 격자 정보 부가함
					.hashJoin("block_cd", OUTPUT(ANALY_BLOCK_RATIO), "block_cd",
								"param.*,avg", INNER_JOIN)
					
					// 비율 값 반영
					.update("avg *= portion")
					
					// 격자별 평균 유동인구을 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"), SUM("avg").as("avg"))
						
					.store(tempOutDsId, FORCE)
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(analyId + "/집계", plan);
		marmot.addAnalysis(anal1, true);
		subCompos.add(anal1.getId());

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(tempOutDsId);
		params.outputDataset(outDsId);
		params.inputFeatureColumns("avg");
		params.outputFeatureColumns("normalized");
		ModuleAnalysis anal2 = new ModuleAnalysis(analyId + "/표준화", "normalize", params.toMap());
		marmot.addAnalysis(anal2, true);
		subCompos.add(anal2.getId());
				
//		SystemAnalysis anal3 = SystemAnalysis.deleteDataSet(analyId + "/집계삭제", tempOutDsId);
//		marmot.addAnalysis(anal3, true);
//		subCompos.add(anal3.getId());
		
		CompositeAnalysis anal = new CompositeAnalysis(analyId, subCompos);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());
	}

	private static void gridCardSales(MarmotRuntime marmot, List<String> compIdList) {
		List<String> subCompIdList = Lists.newArrayList();
		
		List<String> DS_IDS = Lists.newArrayList();
		for ( int year: YEARS ) {
			gridCardSales(marmot, year, subCompIdList);
			DS_IDS.add(OUTPUT(ANALY_CARD, year));
		}
		
		Plan plan;
		plan = marmot.planBuilder("격자별_카드매출_통합")
					.load(DS_IDS, LoadOptions.DEFAULT)
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
										AVG("normalized").as("normalized"))
					.update("normalized = normalized / " + YEARS.length)
					.store(OUTPUT(ANALY_CARD), FORCE)
					.build();
		PlanAnalysis panal = new PlanAnalysis(ANALY_CARD + "/년도별_표준값_병합", plan);
		marmot.addAnalysis(panal, true);
		subCompIdList.add(panal.getId());
		
//		SystemAnalysis sanal = SystemAnalysis.deleteDataSet(ANALY_CARD + "/년도별_표준값_삭제",
//															DS_IDS);
//		marmot.addAnalysis(sanal, true);
//		subCompIdList.add(sanal.getId());
		
		CompositeAnalysis anal = new CompositeAnalysis(ANALY_CARD, subCompIdList);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());
	}
	
	private static void gridCardSales(MarmotRuntime marmot, int year, List<String> compIdList) {
		String analyId = ANALY_CARD + "/" + year;
		String planName = String.format("%d년도 격자별_카드매출 집계", year);
		String inDsId = String.format(CARD_BYTIME, year);
		String outDsId = OUTPUT(ANALY_CARD, year);
		String tempOutDsId = TEMP_OUTPUT(ANALY_CARD, year);
		
		List<String> subCompos = Lists.newArrayList();

		String sumExpr = IntStream.range(0, 24)
									.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
									.collect(Collectors.joining("+"));

		Plan plan;
		plan = marmot.planBuilder(planName)
					// 카드매출을  읽는다.
					.load(inDsId)
					
					// 서울지역 데이터만 선택
					.filter("block_cd.startsWith('11')")
					
					// 하루동안의 카드매출 합계를 계산
					.defineColumn("amount:double", sumExpr)
					
					// 집계구별 일간 카드매출 합계 계산
					.aggregateByGroup(Group.ofKeys("block_cd").workerCount(17),
										AVG("amount").as("amount"))

					// 격자 정보 부가함
					.hashJoin("block_cd", OUTPUT(ANALY_BLOCK_RATIO),
								"block_cd", "param.*,amount", INNER_JOIN)

					// 비율 값 반영
					.update("amount *= portion")
					
					// 격자별 평균 유동인구을 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
										SUM("amount").as("amount"))
						
					.store(tempOutDsId, FORCE)
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(analyId + "/집계", plan);
		marmot.addAnalysis(anal1, true);
		subCompos.add(anal1.getId());

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(tempOutDsId);
		params.outputDataset(outDsId);
		params.inputFeatureColumns("amount");
		params.outputFeatureColumns("normalized");
		ModuleAnalysis anal2 = new ModuleAnalysis(analyId + "/표준화", "normalize", params.toMap());
		marmot.addAnalysis(anal2, true);
		subCompos.add(anal2.getId());
		
//		SystemAnalysis anal3 = SystemAnalysis.deleteDataSet(analyId + "/집계삭제", tempOutDsId);
//		marmot.addAnalysis(anal3, true);
//		subCompos.add(anal3.getId());
		
		CompositeAnalysis anal = new CompositeAnalysis(analyId, subCompos);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());
	}
	
	private static void mergeAll(MarmotRuntime marmot, List<String> components) {
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
		plan = marmot.planBuilder("격자별_유동인구_카드매출_택시승하차_표준값_합계")
					.loadHashJoin(OUTPUT(ANALY_CARD), "cell_id", OUTPUT(ANALY_FLOW_POP), "cell_id",
									"left.*,right.{cell_id as right_cell_id,"
									+ "normalized as right_normalized}",
									FULL_OUTER_JOIN(51))
					.update(merge1)
					.project("cell_id,normalized")
					
					.hashJoin("cell_id", OUTPUT(ANALY_TAXI_LOG), "cell_id",
							"*,param.{cell_id as param_cell_id,"
									+ "normalized as param_normalized}", FULL_OUTER_JOIN(11))
					.update(merge2)
					.project("cell_id,normalized as value")
					
					.store(OUTPUT(ANALY_MERGE), FORCE)
					.build();
		PlanAnalysis anal = new PlanAnalysis(ANALY_MERGE, plan);
		marmot.addAnalysis(anal, true);
		components.add(anal.getId());
	}
	
	private static void attachGeom(MarmotRuntime marmot, List<String> components) {
		DataSet blocks = marmot.getDataSet(BLOCKS);
		GeometryColumnInfo gcInfo = blocks.getGeometryColumnInfo();
		String outJoinCols = String.format("%s,cell_id,cell_pos,param.value", gcInfo.name());
		
		Plan plan;
		plan = marmot.planBuilder("공간데이터 첨부")
					.load(OUTPUT(ANALY_BLOCK_RATIO))
					
					// 격자별로 집계구 겹침 영역 union
					.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_pos"),
										UNION_GEOM(gcInfo.name()))
					
					// 격자별로 결과 데이터 병합
					.hashJoin("cell_id", OUTPUT(ANALY_MERGE), "cell_id", outJoinCols, INNER_JOIN)
					
					.store(RESULT, FORCE(gcInfo))
					.build();
		PlanAnalysis anal = new PlanAnalysis(ANALY_ATTACH_GEOM, plan);
		marmot.addAnalysis(anal, true);
		components.add(anal.getId());
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
	
	private static String TEMP_OUTPUT(String analId, int year) {
		return "/tmp/" + analId + "_연도별/" + year + "_집계";
	}
}
