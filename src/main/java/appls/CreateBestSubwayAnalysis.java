package appls;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.JoinOptions.FULL_OUTER_JOIN;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.exec.CompositeAnalysis;
import marmot.exec.MarmotAnalysis;
import marmot.exec.ModuleAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.exec.SystemAnalysis;
import marmot.optor.AggregateFunction;
import marmot.optor.JoinOptions;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import marmot.process.NormalizeParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Size2d;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateBestSubwayAnalysis {
	private static final String SID = "구역/시도";
	private static final String STATIONS = "교통/지하철/역사";
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String BLOCKS = "구역/지오비전_집계구";
	private static final String FLOW_POP_BYTIME = "주민/유동인구/월별_시간대/2015";
	private static final String CARD_BYTIME = "주민/카드매출/월별_시간대/2015";
	private static final String RESULT = "분석결과/지하철역사_추천/최종결과";

	private static final String TEMP_STATIONS = "/지하철역사_추천/서울지역_지하철역사_버퍼";
	private static final String TEMP_SEOUL = "/지하철역사_추천/서울특별시_영역";
	private static final String TEMP_BLOCK_RATIO = "/지하철역사_추천/집계구_비율";
	private static final String TEMP_TAXI_LOG_GRID = "/지하철역사_추천/격자별_택시승하차_집계";
	private static final String TEMP_NORMAL_TAXI_LOG_GRID = "/지하철역사_추천/격자별_택시_표준_집계";
	private static final String TEMP_FLOW_POP_GRID = "/지하철역사_추천/격자별_유동인구_집계";
	private static final String TEMP_NORMAL_FLOW_POP_GRID = "/지하철역사_추천/격자별_유동인구_표준_집계";
	private static final String TEMP_CARD_GRID = "/지하철역사_추천/격자별_카드매출_집계";
	private static final String TEMP_NORMAL_CARD_GRID = "/지하철역사_추천/격자별_카드매출_표준_집계";
	
	private static final SquareGrid GRID = new SquareGrid(TEMP_SEOUL, new Size2d(300, 300));
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		marmot.deleteAnalysis("/지하철역사_추천", true);
	
		List<MarmotAnalysis> components = new ArrayList<>();
		
		// 서울지역 지하철 역사의 버퍼 작업을 수행한다.
		createSubwayBuffer(marmot, components);
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		createSeoulBoundary(marmot, components);
		
		// 격자별 집계구 비율 계산
		calcBlockRatio(marmot, components);
		
		// 격자별 택시 승하차 수 집계
		gridTaxiLog(marmot, components);
		
		// 격자별 유동인구 집계
		gridFlowPopulation(marmot, components);
		
		// 격자별 카드매출 집계
		gridCardSales(marmot, components);
		
		// 격자별_유동인구_카드매출_택시승하차_비율 합계
		mergeAll(marmot, components);

		components.add(SystemAnalysis.deleteDataSet("/지하철역사_추천/격자별_택시승하차_표준_삭제",
													TEMP_NORMAL_TAXI_LOG_GRID));
		components.add(SystemAnalysis.deleteDataSet("/지하철역사_추천/격자별_유동인구_표준_삭제",
													TEMP_NORMAL_FLOW_POP_GRID));
		components.add(SystemAnalysis.deleteDataSet("/지하철역사_추천/격자별_카드매출_표준_삭제",
													TEMP_NORMAL_CARD_GRID));
		
		List<String> compIds = new ArrayList<>();
		for ( MarmotAnalysis anal: components ) {
			marmot.addAnalysis(anal);
			compIds.add(anal.getId());
		}
		marmot.addAnalysis(new CompositeAnalysis("/지하철역사_추천", compIds));
	}
	
	private static void createSubwayBuffer(MarmotRuntime marmot, List<MarmotAnalysis> components) {
		DataSet stations = marmot.getDataSet(STATIONS);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();
		
		Plan plan;
		
		// 서울지역 지하철 역사를 구하고 1km 버퍼를 구한다.
		plan = marmot.planBuilder("서울지역_지하철역사_버퍼")
					.load(STATIONS)
					.filter("sig_cd.substring(0,2) == '11'")
					.buffer(gcInfo.name(), 1000)
					.project("the_geom")
					.store(TEMP_STATIONS, FORCE(gcInfo))
					.build();
		components.add(new PlanAnalysis(TEMP_STATIONS, plan));

		// 서울지역 지하철 역사에 대한 공간색인을 생성한다.
		components.add(SystemAnalysis.clusterDataSet(TEMP_STATIONS + "_색인", TEMP_STATIONS));
	}
	
	private static void createSeoulBoundary(MarmotRuntime marmot, List<MarmotAnalysis> components) {
		Plan plan;
		
		DataSet stations = marmot.getDataSet(SID);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();

		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		plan = marmot.planBuilder("서울특별시_영역_추출")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.store(TEMP_SEOUL, FORCE(gcInfo))
					.build();
		components.add(new PlanAnalysis(TEMP_SEOUL, plan));
		
		// 서울지역 버퍼에 대한 공간색인을 생성한다.
		components.add(SystemAnalysis.clusterDataSet(TEMP_SEOUL + "_색인", TEMP_SEOUL));
	}
	
	private static void calcBlockRatio(MarmotRuntime marmot, List<MarmotAnalysis> components) {
		DataSet stations = marmot.getDataSet(BLOCKS);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();
		

		Plan plan;
		plan = marmot.planBuilder("격자별_집계구_비율")
					.load(BLOCKS)
					.filter("block_cd.substring(0,2) == '11'")
					.differenceJoin(gcInfo.name(), TEMP_STATIONS)
					.assignGridCell(gcInfo.name(), GRID, false)
					.intersection(gcInfo.name(), "cell_geom", "overlap")
					.defineColumn("ratio:double", "ratio = ST_Area(overlap) / ST_Area(cell_geom)")
					.project("cell_geom as the_geom, cell_id, cell_pos, block_cd, ratio")
					.store(TEMP_BLOCK_RATIO, FORCE(gcInfo))
					.build();
		components.add(new PlanAnalysis(TEMP_BLOCK_RATIO, plan));
		
		// 격자별_집계구_비율에 대한 공간색인을 생성한다.
		components.add(SystemAnalysis.clusterDataSet(TEMP_BLOCK_RATIO + "_색인", TEMP_BLOCK_RATIO));
	}
	
	private static void gridTaxiLog(MarmotRuntime marmot, List<MarmotAnalysis> components) {
		DataSet stations = marmot.getDataSet(TAXI_LOG);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder("격자단위_승하차 집계")
					// 택시 로그를  읽는다.
					.load(TAXI_LOG)
					
					// 승하차 로그만 선택한다.
					.filter("status == 1 || status == 2")
					
					// 로그에 격자 정보 부가함
					.spatialJoin("the_geom", TEMP_BLOCK_RATIO, "param.*,*-{the_geom}")

					// 격자별로 택시 승하차 로그수를 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("the_geom,ratio"),
										AggregateFunction.COUNT())
					
					// 각 격자에 배정된 비율만큼 로그 수를 보정함
					.defineColumn("count:double", "count * ratio")
					
					.project("the_geom, cell_id, count")
					
					.store(TEMP_TAXI_LOG_GRID, FORCE(gcInfo))
					.build();
		components.add(new PlanAnalysis(TEMP_TAXI_LOG_GRID, plan));

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(TEMP_TAXI_LOG_GRID);
		params.outputDataset(TEMP_NORMAL_TAXI_LOG_GRID);
		params.inputFeatureColumns("count");
		params.outputFeatureColumns("normalized");
		components.add(new ModuleAnalysis(TEMP_TAXI_LOG_GRID + "_표준화", "normalize",
											params.toMap()));
		
		components.add(SystemAnalysis.deleteDataSet(TEMP_TAXI_LOG_GRID + "_삭제",
													TEMP_TAXI_LOG_GRID));
	}
	
	private static void gridFlowPopulation(MarmotRuntime marmot, List<MarmotAnalysis> components) {
		DataSet stations = marmot.getDataSet(FLOW_POP_BYTIME);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();

		String sumExpr = IntStream.range(0, 24)
									.mapToObj(idx -> String.format("avg_%02dtmst", idx))
									.collect(Collectors.joining("+"));
		String avgExpr = String.format("(%s) / 24", sumExpr);

		Plan plan;
		plan = marmot.planBuilder("격자단위 유동인구 수집")
					// 유동인구를  읽는다.
					.load(FLOW_POP_BYTIME)
					
					// 서울지역 데이터만 선택
					.filter("block_cd.startsWith('11')")
					
					// 하루동안의 평균 유동인구를 계산
					.defineColumn("daily_avg:double", avgExpr)

					// 격자 정보 부가함
					.hashJoin("block_cd", TEMP_BLOCK_RATIO, "block_cd", "param.*,daily_avg",
								JoinOptions.INNER_JOIN(23))
					
					// 격자별 평균 유동인구을 계산한다.
					.aggregateByGroup(Group.ofKeys("block_cd")
											.tags(String.format("%s,ratio,cell_id", gcInfo.name())),
										AVG("daily_avg").as("avg"))
					
					// 비율 값 반영
					.update("avg *= ratio")
						
					.project("the_geom, cell_id, avg")
						
					.store(TEMP_FLOW_POP_GRID, FORCE(gcInfo))
					.build();
		components.add(new PlanAnalysis(TEMP_FLOW_POP_GRID, plan));

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(TEMP_FLOW_POP_GRID);
		params.outputDataset(TEMP_NORMAL_FLOW_POP_GRID);
		params.inputFeatureColumns("avg");
		params.outputFeatureColumns("normalized");
		components.add(new ModuleAnalysis(TEMP_FLOW_POP_GRID + "_표준화", "normalize",
											params.toMap()));
		
		components.add(SystemAnalysis.deleteDataSet(TEMP_FLOW_POP_GRID + "_삭제",
													TEMP_FLOW_POP_GRID));
	}
	
	private static void gridCardSales(MarmotRuntime marmot, List<MarmotAnalysis> components) {
		DataSet ds = marmot.getDataSet(BLOCKS);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		String sumExpr = IntStream.range(0, 24)
									.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
									.collect(Collectors.joining("+"));

		Plan plan;
		plan = marmot.planBuilder("격자단위 카드매출 수집")
					// 카드매출을  읽는다.
					.load(CARD_BYTIME)
					
					// 서울지역 데이터만 선택
					.filter("block_cd.startsWith('11')")
					
					// 하루동안의 카드매출 합계를 계산
					.defineColumn("daily_sum:double", sumExpr)

					// 격자 정보 부가함
					.hashJoin("block_cd", TEMP_BLOCK_RATIO, "block_cd", "param.*,daily_sum",
								JoinOptions.INNER_JOIN(51))
					
					// 격자별 평균 유동인구을 계산한다.
					.aggregateByGroup(Group.ofKeys("block_cd")
											.tags(String.format("%s,cell_id,ratio", gcInfo.name())),
										SUM("daily_sum").as("amount"))
					
					// 비율 값 반영
					.update("amount *= ratio")
						
					.project("the_geom, cell_id, amount")
						
					.store(TEMP_CARD_GRID, FORCE(gcInfo))
					.build();
		components.add(new PlanAnalysis(TEMP_CARD_GRID, plan));

		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(TEMP_CARD_GRID);
		params.outputDataset(TEMP_NORMAL_CARD_GRID);
		params.inputFeatureColumns("amount");
		params.outputFeatureColumns("normalized");
		components.add(new ModuleAnalysis(TEMP_CARD_GRID + "_표준화", "normalize",
											params.toMap()));
		
		components.add(SystemAnalysis.deleteDataSet(TEMP_CARD_GRID + "_삭제", TEMP_CARD_GRID));
	}
	
	private static void mergeAll(MarmotRuntime marmot, List<MarmotAnalysis> components) {
		DataSet ds = marmot.getDataSet(BLOCKS);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		String merge1 = "if ( normalized == null ) {"
					+ "		the_geom = right_geom;"
					+ "		cell_id = right_cell_id;"
					+ "		normalized = 0;"
					+ "} else if ( right_normalized == null ) {"
					+ "		right_normalized = 0;"
					+ "}"
					+ "normalized = normalized + right_normalized;";
		String merge2 = "if ( normalized == null ) {"
					+ "		the_geom = param_geom;"
					+ "		cell_id = param_cell_id;"
					+ "		normalized = 0;"
					+ "} else if ( param_normalized == null ) {"
					+ "		param_normalized = 0;"
					+ "}"
					+ "normalized = normalized + param_normalized;";
		
		Plan plan;
		plan = marmot.planBuilder("격자별_유동인구_카드매출_택시승하차_비율 합계")
					.loadHashJoin(TEMP_NORMAL_CARD_GRID, "cell_id",
									TEMP_NORMAL_FLOW_POP_GRID, "cell_id",
									"left.*,right.{the_geom as right_geom, cell_id as right_cell_id,"
									+ "normalized as right_normalized}",
									FULL_OUTER_JOIN(51))
					.update(merge1)
					.project("the_geom,cell_id,normalized")
					
					.hashJoin("cell_id", TEMP_NORMAL_TAXI_LOG_GRID, "cell_id",
							"*,param.{the_geom as param_geom,cell_id as param_cell_id,"
							+ "normalized as param_normalized}", FULL_OUTER_JOIN(51))
					.update(merge2)
					
					.project("the_geom,cell_id,normalized as value")
					.store(RESULT, FORCE(gcInfo))
					.build();
		components.add(new PlanAnalysis("/지하철역사_추천/격자별_비율합계", plan));
	}
}
