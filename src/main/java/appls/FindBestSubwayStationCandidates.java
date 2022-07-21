package appls;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.JoinOptions.FULL_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import utils.Size2d;
import utils.StopWatch;

import common.SampleUtils;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.analysis.module.NormalizeParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindBestSubwayStationCandidates {
	private static final String SID = "구역/시도";
	private static final String TAXI_LOG = "나비콜/택시로그";
	private static final String FLOW_POP_BYTIME = "주민/유동인구/2015/월별_시간대";
	private static final String STATIONS = "교통/지하철/역사";
	private static final String RESULT = "분석결과/지하철역사_추천/최종결과";
	private static final Size2d CELL_SIZE = new Size2d(500, 500);

	private static final String TEMP_SEOUL = "분석결과/지하철역사_추천/서울지역";
	private static final String TEMP_SUBWAY = "분석결과/지하철역사_추천/서울역사_버퍼";
	private static final String TEMP_SEOUL_TAXI_LOG_GRID = "분석결과/지하철역사_추천/역사외_지역/택시로그/격자별_집계";
	private static final String TEMP_SEOUL_FLOW_POP_GRID = "분석결과/지하철역사_추천/역사외_지역/유동인구/격자별_집계";
	private static final String TEMP_FLOW_POP = "분석결과/지하철역사_추천/유동인구";
	private static final String TEMP_TAXI_LOG = "분석결과/지하철역사_추천/택시로그";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch totalElapsed = StopWatch.start();
		
		DataSet result;
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		DataSet seoul = getSeoulBoundary(marmot, TEMP_SEOUL);
		
		// 서울지역 지하철 역사를 구하고 1km 버퍼를 구한다.
		DataSet subway = bufferSubwayStations(marmot, TEMP_SUBWAY);
		
//		result = gridFlowPopulation(marmot, seoul, subway, TEMP_FLOW_POP);
		result = gridTaxiLog(marmot, seoul, subway, TEMP_TAXI_LOG);
		result = mergePopAndTaxi(marmot, RESULT);
		totalElapsed.stop();
		
		System.out.printf("분석종료: 결과=%s(%d건), 총소요시간==%s%n",
						result, result.getRecordCount(), totalElapsed.getElapsedMillisString());
		
		marmot.deleteDataSet(TEMP_FLOW_POP);
		marmot.deleteDataSet(TEMP_TAXI_LOG);

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + totalElapsed.getElapsedMillisString());
	}
	
	private static DataSet getSeoulBoundary(MarmotRuntime marmot, String outDsId) {
		StopWatch watch = StopWatch.start();
		
		System.out.print("분석 단계: '전국_법정시도경계'에서 서울특별시 영역 추출 -> ");
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(SID).getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		System.out.printf("1건, 소요시간=%s%n", watch.getElapsedMillisString());
		return marmot.getDataSet(outDsId);
	}
	
	private static DataSet bufferSubwayStations(MarmotRuntime marmot, String outDsId) {
		System.out.print("분석 단계: '전국_지하쳘_역사' 중 서울 소재 역사 추출 후 1KM 버퍼 계산 -> ");
		
		StopWatch watch = StopWatch.start();
		
		// 서울지역 지하철 역사를 구하고 1km 버퍼를 구한다.
		DataSet stations = marmot.getDataSet(STATIONS);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();

		Plan plan;
		plan = Plan.builder("서울지역 지하철역사 1KM 버퍼")
					.load(STATIONS)
					.filter("sig_cd.substring(0,2) == '11'")
					.buffer(gcInfo.name(), 1000)
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%s%n",
							outDsId, result.getRecordCount(), watch.getElapsedMillisString());
		
		return result;
	}
	
	private static DataSet gridFlowPopulation(MarmotRuntime marmot, DataSet seoul, DataSet subway,
												String outDsId) {
		StopWatch watch = StopWatch.start();
		
		System.out.print("분석 단계: '전국_월별_유동인구'에서 '500mX500m 격자단위 유동인구' 집계 -> ");
		GeometryColumnInfo gcInfo =  marmot.getDataSet(FLOW_POP_BYTIME).getGeometryColumnInfo();
		String sumExpr = IntStream.range(0, 24)
									.mapToObj(idx -> String.format("avg_%02dtmst", idx))
									.collect(Collectors.joining("+"));

		Plan plan;
		plan = Plan.builder("'500mX500m 격자단위 유동인구' 집계")
					// 서울시 영역만 추출한다.
					.query(FLOW_POP_BYTIME, seoul.getId())
					
					// 모든 지하철 역사로부터 1km 이상 떨어진 로그 데이터만 선택한다.
					.spatialSemiJoin(gcInfo.name(), subway.getId(), SpatialJoinOptions.NEGATED)
					
					// 각 시간대의 유동인구를 모두 더해 하루동안의 유동인구를 계산
					.defineColumn("day_total:double", sumExpr)
					
					// 각 달의 소지역의 연간 유동인구 평균을 계산한다.
					.aggregateByGroup(Group.ofKeys("block_cd").tags(gcInfo.name()), AVG("day_total"))
						
					// 각 소지역이 폼함되는 사각 셀을  부가한다.
					.assignGridCell(gcInfo.name(), new SquareGrid(seoul.getBounds(), CELL_SIZE), false)
					.project("cell_geom as the_geom, cell_id, cell_pos, avg")
					
					// 사각 그리드 셀 단위로 그룹핑하고, 각 그룹에 속한 유동인구를 모두 더한다.
					.aggregateByGroup(Group.ofKeys("cell_id").withTags("the_geom"),
										SUM("avg").as("avg"))
					
					// 결과 저장
					.store(TEMP_SEOUL_FLOW_POP_GRID, FORCE(gcInfo))
					.build();
		
		try {
			marmot.execute(plan);
			
			DataSet result = marmot.getDataSet(TEMP_SEOUL_FLOW_POP_GRID);
			System.out.printf("%s(%d건), 소요시간=%s%n",
								TEMP_SEOUL_FLOW_POP_GRID, result.getRecordCount(),
								watch.getElapsedMillisString());

			
			System.out.print("분석 단계: '500mX500m 격자단위 표준 유동인구' 집계 -> ");
			watch = StopWatch.start();
			
			NormalizeParameters params = new NormalizeParameters();
			params.inputDataset(TEMP_SEOUL_FLOW_POP_GRID);
			params.outputDataset(outDsId);
			params.inputFeatureColumns("avg");
			params.outputFeatureColumns("normalized");
			marmot.executeProcess("normalize", params.toMap());
			
			result = marmot.getDataSet(outDsId);
			System.out.printf("%s(%d건), 소요시간=%s%n", outDsId, result.getRecordCount(),
								watch.getElapsedMillisString());
			
			return result;
		}
		finally {
			marmot.deleteDataSet(TEMP_SEOUL_FLOW_POP_GRID);
		}
	}
	
	private static DataSet gridTaxiLog(MarmotRuntime marmot, DataSet seoul, DataSet subway,
										String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.print("분석 단계: '택시 운행 로그'에서 500mX500m 격자단위 승하차 집계 -> ");
		
		GeometryColumnInfo gcInfo =  marmot.getDataSet(TAXI_LOG).getGeometryColumnInfo();
		
		// 택시 운행 로그 기록에서 성울시 영역부분에서 승하차 로그 데이터만 추출한다.
		Plan plan;
		plan = Plan.builder("택시승하차 로그 집계")
					// 택시 로그를  읽는다.
					.load(TAXI_LOG)
					
					// 승하차 로그만 선택한다.
					.filter("status == 1 || status == 2")
					
					// 서울특별시 영역만의 로그만 선택한다.
					.filterSpatially(gcInfo.name(), INTERSECTS, seoul.getId())
					// 불필요한 컬럼 제거
					.project("the_geom")
					
					// 모든 지하철 역사로부터 1km 이상 떨어진 로그 데이터만 선택한다.
					.spatialSemiJoin(gcInfo.name(), subway.getId(), SpatialJoinOptions.NEGATED)
					
					// 각 로그 위치가 포함된 사각 셀을  부가한다.
					.assignGridCell(gcInfo.name(), new SquareGrid(seoul.getBounds(), CELL_SIZE), false)
					.project("cell_geom as the_geom, cell_id, cell_pos")
					
					// 사각 그리드 셀 단위로 그룹핑하고, 각 그룹에 속한 레코드 수를 계산한다.
					.aggregateByGroup(Group.ofKeys("cell_id").tags("the_geom"), COUNT())
					
					.store(TEMP_SEOUL_TAXI_LOG_GRID, FORCE(gcInfo))
					.build();
		try {
			marmot.execute(plan);
			
			DataSet result = marmot.getDataSet(TEMP_SEOUL_TAXI_LOG_GRID);
			System.out.printf("%s(%d건), 소요시간=%s%n", TEMP_SEOUL_TAXI_LOG_GRID,
								result.getRecordCount(), watch.getElapsedMillisString());

			
			System.out.print("분석 단계: '500mX500m 격자단위 표준 택시 승하차' 집계 -> ");
			watch = StopWatch.start();
			
			NormalizeParameters params = new NormalizeParameters();
			params.inputDataset(TEMP_SEOUL_TAXI_LOG_GRID);
			params.outputDataset(outDsId);
			params.inputFeatureColumns("count");
			params.outputFeatureColumns("normalized");
			marmot.executeProcess("normalize", params.toMap());

			result = marmot.getDataSet(outDsId);
			System.out.printf("%s(%d건), 소요시간=%s%n", outDsId, result.getRecordCount(),
								watch.getElapsedMillisString());
			
			return result;
		}
		finally {
			marmot.deleteDataSet(TEMP_SEOUL_TAXI_LOG_GRID);
		}
	}
	
	private static DataSet mergePopAndTaxi(MarmotRuntime marmot, String outDsId) {
		System.out.print("분석 단계: '500mX500m 격자단위 표준 유동인구'과 '500mX500m 격자단위 표준 택시 승하차' 합계 -> ");
		
		Plan plan;
		StopWatch watch = StopWatch.start();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(TEMP_FLOW_POP).getGeometryColumnInfo();
		String expr = "if ( normalized == null ) {"
					+ "		the_geom = param_geom;"
					+ "		cell_id = param_cell_id;"
					+ "		normalized = 0;"
					+ "} else if ( param_normalized == null ) {"
					+ "		param_normalized = 0;"
					+ "}"
					+ "normalized = normalized + param_normalized;";
		
		plan = Plan.builder("그리드 셀단위 유동인구 비율과 택시 승하차 로그 비율 합계 계산")
					.load(TEMP_FLOW_POP)
					.hashJoin("cell_id", TEMP_TAXI_LOG, "cell_id",
							"the_geom,cell_id,normalized,"
							+ "param.{the_geom as param_geom,cell_id as param_cell_id,"
							+ "normalized as param_normalized}", FULL_OUTER_JOIN)
					.update(expr)
					.project("the_geom,cell_id,normalized as value")
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%s%n",
							outDsId, result.getRecordCount(), watch.getElapsedMillisString());
		
		return result;
	}
}
