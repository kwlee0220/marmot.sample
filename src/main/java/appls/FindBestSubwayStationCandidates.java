package appls;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.JoinType.OUTER_JOIN;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.optor.JoinOptions;
import marmot.process.AttachPortionParameters;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.DimensionDouble;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindBestSubwayStationCandidates {
	private static final String SID = "구역/시도";
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String FLOW_POP_BYTIME = "주민/유동인구/월별_시간대/2015";
	private static final String STATIONS = "교통/지하철/역사";
	private static final String RESULT = "분석결과/최종결과";
	private static final String GEOM_COL = "the_geom";
	private static final String SRID = "EPSG:5186";
	private static final DimensionDouble CELL_SIZE = new DimensionDouble(500, 500);
	
	private static final String TEMP_STATIONS = "분석결과/지하철역사_버퍼_그리드";
	private static final String TEMP_SEOUL_TAXI_LOG = "분석결과/역사외_지역/택시로그/집계";
	private static final String TEMP_SEOUL_TAXI_LOG_GRID = "분석결과/역사외_지역/택시로그/그리드별_집계";
	private static final String TEMP_SEOUL_FLOW_POP_BLOCK = "분석결과/역사외_지역/유동인구/소지역별_집계";
	private static final String TEMP_SEOUL_FLOW_POP_GRID = "분석결과/역사외_지역/유동인구/그리드별_집계";
	private static final String TEMP_FLOW_POP = "분석결과/후보그리드/유동인구";
	private static final String TEMP_TAXI_LOG = "분석결과/후보그리드/택시로그";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		Plan plan;
		DataSet result;

		// 서울지역 지하철 역사를 구하고 1km 버퍼를 구한다.
		bufferSubwayStations(marmot, TEMP_STATIONS);
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		Geometry seoul = getSeoulBoundary(marmot);
		
		gridFlowPopulation(marmot, seoul, TEMP_FLOW_POP);
		gridTaxiLog(marmot, seoul, TEMP_TAXI_LOG);
		
		String expr = "if ( portion == null ) {"
					+ "		the_geom = param_geom;"
					+ "		cell_id = param_cell_id;"
					+ "		portion = 0;"
					+ "} else if ( param_portion == null ) {"
					+ "		param_portion = 0;"
					+ "}"
					+ "portion = portion + param_portion;";
		
		plan = marmot.planBuilder("그리드 셀단위 유동인구 비율과 택시 승하차 로그 비율 합계 계산")
					.load(TEMP_FLOW_POP)
					.join("cell_id", TEMP_TAXI_LOG, "cell_id",
							"the_geom,cell_id,portion,"
							+ "param.{the_geom as param_geom,cell_id as param_cell_id,"
							+ "portion as param_portion}",
							new JoinOptions().joinType(OUTER_JOIN))
					.update(expr)
					.project("the_geom,cell_id,portion as value")
					.store(RESULT)
					.build();
		
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		result = marmot.createDataSet(RESULT, schema, GEOM_COL, SRID, true);
		marmot.execute(plan);
		watch.stop();
		
		System.out.printf("종료: 그리드 셀단위 유동인구 비율과 택시 승하차 로그 비율 합계, output=%s, elapsed=%s%n",
						RESULT, watch.getElapsedTimeString());
		
//		marmot.deleteDataSet(TEMP_FLOW_POP);
//		marmot.deleteDataSet(TEMP_TAXI_LOG);
//		marmot.deleteDataSet(TEMP_STATIONS);

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedTimeString());
	}
	
	private static Geometry getSeoulBoundary(MarmotClient marmot) {
		Plan plan;
		
		DataSet sid = marmot.getDataSet(SID);
		plan = marmot.planBuilder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.build();
		return marmot.executeLocally(plan).toList().get(0)
					.getGeometry(sid.getGeometryColumn());
	}
	
	private static DataSet bufferSubwayStations(MarmotClient marmot, String output) {
		Plan plan;
		StopWatch watch = StopWatch.start();
		
		// 서울지역 지하철 역사를 구하고 1km 버퍼를 구한다.
		DataSet stations = marmot.getDataSet(STATIONS);
		String geomCol = stations.getGeometryColumn();
		String srid = stations.getSRID();
		
		plan = marmot.planBuilder("서울지역 지하철역사 1KM 버퍼")
					.load(STATIONS)
					.filter("sig_cd.substring(0,2) == '11'")
					.buffer(geomCol, geomCol, 1000)
					.store(output)
					.build();

		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		DataSet result = marmot.createDataSet(output, schema, geomCol, srid, true);
		marmot.execute(plan);
		
		System.out.printf("종료: 서울지역 지하철역사 1KM 버퍼, output=%s, elapsed=%s%n",
							output, watch.getElapsedTimeString());
		
		return result;
	}
	
	private static void gridFlowPopulation(MarmotClient marmot, Geometry seoul, String output) {
		Plan plan;
		StopWatch watch = StopWatch.start();
		
		DataSet input = marmot.getDataSet(FLOW_POP_BYTIME);
		String geomCol = input.getGeometryColumn();
		String srid = input.getSRID();

		Envelope bounds = seoul.getEnvelopeInternal();
		String sumExpr = IntStream.range(0, 24)
									.mapToObj(idx -> String.format("avg_%02dtmst", idx))
									.collect(Collectors.joining("+", "avg = ", ""));

		final String tmplt = "if (avg_%02dtmst == null) { avg_%02dtmst = 0; }%n";
		String expr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format(tmplt, idx, idx))
								.collect(Collectors.joining());
		
		RecordSchema schema;
		plan = marmot.planBuilder("소지역단위 유동인구 집계")
					// 서울시 영역만 추출한다.
					.load(FLOW_POP_BYTIME, INTERSECTS, seoul)
					// 모든 지하철 역사로부터 1km 이상 떨어진 로그 데이터만 선택한다.
					.spatialSemiJoin("the_geom", TEMP_STATIONS, INTERSECTS, true)
					// 일부 시간대 유동인구가 null인 경우 0으로 치환한다.
					.update(expr)
					// 각 시간대의 유동인구를 모두 더해 하루동안의 유동인구를 계산
					.expand("avg:double", sumExpr)
					.project("the_geom,std_ym,block_cd,avg")
					// 각 달의 소지역의 평균 유동인구를 계산한다.
					.groupBy("block_cd")
						.taggedKeyColumns(geomCol)
						.aggregate(AVG("avg"))
					.store(TEMP_SEOUL_FLOW_POP_BLOCK)
					.build();
		
		try {
			schema = marmot.getOutputRecordSchema(plan);
			marmot.createDataSet(TEMP_SEOUL_FLOW_POP_BLOCK, schema, geomCol, srid, true);
			marmot.execute(plan);
			
			System.out.printf("종료: 소지역단위 유동인구 집계, output=%s, elapsed=%s%n",
							TEMP_SEOUL_FLOW_POP_BLOCK, watch.getElapsedTimeString());
	
			watch = StopWatch.start();
			plan = marmot.planBuilder("그리드 셀단위 유동인구 집계")
						.load(TEMP_SEOUL_FLOW_POP_BLOCK)
						// 각 로그 위치가 포함된 사각 셀을  부가한다.
						.assignSquareGridCell(geomCol, bounds, CELL_SIZE)
						.project("cell_geom as the_geom, cell_id, cell_pos, avg")
						// 사각 그리드 셀 단위로 그룹핑하고, 각 그룹에 속한 유동인구를 모두 더한다.
						.groupBy("cell_id")
							.taggedKeyColumns("the_geom")
							.aggregate(SUM("avg").as("avg"))
						.store(TEMP_SEOUL_FLOW_POP_GRID)
						.build();
			schema = marmot.getOutputRecordSchema(plan);
			marmot.createDataSet(TEMP_SEOUL_FLOW_POP_GRID, schema, GEOM_COL, srid, true);
			marmot.execute(plan);
			System.out.printf("종료: 그리드 셀단위 유동인구 집계, output=%s, elapsed=%s%n",
								TEMP_SEOUL_FLOW_POP_GRID, watch.getElapsedTimeString());

			watch = StopWatch.start();
			AttachPortionParameters params = new AttachPortionParameters();
			params.inputDataset(TEMP_SEOUL_FLOW_POP_GRID);
			params.outputDataset(output);
			params.inputFeatureColumns("avg");
			params.outputFeatureColumns("portion");
			marmot.executeProcess("attach_portion", params.toMap());
			System.out.printf("종료: 그리드 셀단위 유동인구 비율 계산, output=%s, elapsed=%s%n",
								output, watch.getElapsedTimeString());
		}
		finally {
//			marmot.deleteDataSet(TEMP_SEOUL_FLOW_POP_BLOCK);
//			marmot.deleteDataSet(TEMP_SEOUL_FLOW_POP_GRID);
		}
	}
	
	private static void gridTaxiLog(MarmotClient marmot, Geometry seoul, String output) {
		Plan plan;
		
		DataSet taxi = marmot.getDataSet(TAXI_LOG);
		String geomCol = taxi.getGeometryColumn();
		String srid = taxi.getSRID();
		
		StopWatch watch = StopWatch.start();
		
		// 택시 운행 로그 기록에서 성울시 영역부분에서 승하차 로그 데이터만 추출한다.
		Envelope bounds = seoul.getEnvelopeInternal();
		plan = marmot.planBuilder("택시승하차 로그 집계")
					// 택시 로그를  읽는다.
					.load(TAXI_LOG)
					// 승하차 로그만 선택한다.
					.filter("status == 1 || status == 2")
					// 서울특별시 영역만의 로그만 선택한다.
					.intersects(geomCol, seoul)
					// 모든 지하철 역사로부터 1km 이상 떨어진 로그 데이터만 선택한다.
					.spatialSemiJoin("the_geom", TEMP_STATIONS, INTERSECTS, true)
					.store(TEMP_SEOUL_TAXI_LOG)
					.build();
		
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		marmot.createDataSet(TEMP_SEOUL_TAXI_LOG, schema, geomCol, srid, true);
		marmot.execute(plan);
		System.out.println("종료: 택시승하차 로그 집계, elapsed=" + watch.getElapsedTimeString());
					
		watch = StopWatch.start();
		plan = marmot.planBuilder("그리드 셀단위 택시승하차 로그 집계")
					.load(TEMP_SEOUL_TAXI_LOG)
					// 각 로그 위치가 포함된 사각 셀을  부가한다.
					.assignSquareGridCell(geomCol, bounds, CELL_SIZE)
					.project("cell_geom as the_geom, cell_id, cell_pos")
					// 사각 그리드 셀 단위로 그룹핑하고, 각 그룹에 속한 레코드 수를 계산한다.
					.groupBy("cell_id")
						.taggedKeyColumns("the_geom")
						.aggregate(COUNT())
						
					.store(TEMP_SEOUL_TAXI_LOG_GRID)
					.build();
		try {
			schema = marmot.getOutputRecordSchema(plan);
			marmot.createDataSet(TEMP_SEOUL_TAXI_LOG_GRID, schema, GEOM_COL, srid, true);
			marmot.execute(plan);
			System.out.printf("종료: 그리드 셀단위 택시승하차 로그 집계, output=%s, elapsed=%s%n",
								TEMP_SEOUL_TAXI_LOG_GRID, watch.getElapsedTimeString());
			
			AttachPortionParameters params = new AttachPortionParameters();
			params.inputDataset(TEMP_SEOUL_TAXI_LOG_GRID);
			params.outputDataset(output);
			params.inputFeatureColumns("count");
			params.outputFeatureColumns("portion");
			marmot.executeProcess("attach_portion", params.toMap());
			System.out.printf("종료: 그리드 셀단위 택시승하차 로그비율 계산, output=%s, elapsed=%s%n",
								output, watch.getElapsedTimeString());
		}
		finally {
//			marmot.deleteDataSet(TEMP_SEOUL_TAXI_LOG);
//			marmot.deleteDataSet(TEMP_SEOUL_TAXI_LOG_GRID);
		}
	}
}
