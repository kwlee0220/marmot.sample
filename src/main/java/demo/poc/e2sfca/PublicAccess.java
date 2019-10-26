package demo.poc.e2sfca;

import static marmot.StoreDataSetOptions.FORCE;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.exec.ExternAnalysis;
import marmot.optor.ParseCsvOptions;
import marmot.optor.StoreAsCsvOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PublicAccess {
	private static final String FLOW_POP = "연세대/사업단실증/E2SFCA/유동인구";
	private static final String BUS = "연세대/사업단실증/E2SFCA/버스";
	private static final String SUBWAY = "연세대/사업단실증/E2SFCA/지하철";
	private static final String RESULT = "분석결과/E2SFCA/대중교통_접근성";
	
	private static final String ANALYSIS = "대중교통_접근성";
	private static final String ANALY_FLOWPOP = "대중교통_접근성/강남구_유동인구_준비";
	private static final String ANALY_BUS = "대중교통_접근성/강남구_버스_준비";
	private static final String ANALY_SUBWAY = "대중교통_접근성/강남구_지하철_준비";
	private static final String ANALY_SUBMIT = "대중교통_접근성/분석";
	private static final String ANALY_COLLECT = "대중교통_접근성/결과수집";

	private static final String CSV_FLOWPOP_PATH = "tmp/e2sfca/pop_input.csv";
	private static final String CSV_BUS_PATH = "tmp/e2sfca/bus_input.csv";
	private static final String CSV_SUBWAY_PATH = "tmp/e2sfca/subway_input.csv";
	private static final String CSV_RESULT_PATH = "tmp/e2sfca/e2sfca_output.csv";

	private static final String HEADER_POPFLOW = "STD_YM,BLOCK_CD,X_COORD,Y_COORD,AVG_08TMST,AVG_15TMST,SUM_area";
	private static final String HEADER_BUS = "StationNM,XCOORD,YCOORD,ARSID,Slevel_08,Slevel_15";
	private static final String HEADER_SUBWAY = "station_nm,XCOORD,YCOORD,line_num,Slevel_08,Slevel_15";

	private static final String SPARK_PATH = "/usr/bin/spark-submit";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
//		setGangnamFlowPop(marmot, CSV_FLOWPOP_PATH);
//		setGangnamBus(marmot, CSV_BUS_PATH);
//		setGangnamSubway(marmot, CSV_SUBWAY_PATH);
		execE2SFCA(marmot, CSV_FLOWPOP_PATH, CSV_BUS_PATH, CSV_SUBWAY_PATH, CSV_RESULT_PATH);
//		collectResult(marmot, CSV_RESULT_PATH, RESULT);
//
//		marmot.addAnalysis(new CompositeAnalysis(ANALYSIS, compIdList), true);
	}
	
	private static void setGangnamFlowPop(MarmotRuntime marmot, String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("단계: 강남구 유동인구 데이터 준비 -> ");
		
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("강남구_유동인구_준비")
					.load(FLOW_POP)
					.toXY("the_geom", "XCOORD", "YCOORD")
					.project("std_ym as STD_YM,block_cd as BLOCK_CD,"
							+ "XCOORD as X_COORD,YCOORD as Y_COORD,"
							+ "avg_08tmst as AVG_08TMST,avg_15tmst as AVG_15TMST,"
							+ "sum_area as SUM_area")
					.shard(1)
					.storeAsCsv(outCsvPath, opts)
					.build();
		marmot.execute(plan);
		
		System.out.printf("output=%s, 소요시간=%ss%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void setGangnamBus(MarmotRuntime marmot, String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("단계: 강남구 버스 데이터 준비 -> ");
		
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("강남구_버스정보_추출")
					.load(BUS)
					.toXY("the_geom", "XCOORD", "YCOORD")
					.project(HEADER_BUS)
					.shard(1)
					.storeAsCsv(outCsvPath, opts)
					.build();
		marmot.execute(plan);
		
		System.out.printf("output=%s, 소요시간=%ss%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void setGangnamSubway(MarmotRuntime marmot, String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("단계: 강남구 지하철 데이터 준비 -> ");
		
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("강남구_지하철정보_추출")
					.load(SUBWAY)
					.toXY("the_geom", "XCOORD", "YCOORD")
					.project(HEADER_SUBWAY)
					.shard(1)
					.storeAsCsv(outCsvPath, opts)
					.build();
		marmot.execute(plan);
		
		System.out.printf("output=%s, 소요시간=%ss%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void execE2SFCA(MarmotRuntime marmot, String flowPop, String bus, String subway,
									String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("단계: E2SFCA -> ");
		
		String[] args = new String[] {
			"--class", "main.scala.E2SFCA", "extensions/e2sfca_2.11-yarn_3.0.jar",
			flowPop, bus, subway, outCsvPath,
			HEADER_POPFLOW, HEADER_BUS, HEADER_SUBWAY
		};

		ExternAnalysis anal = new ExternAnalysis(ANALY_SUBMIT, SPARK_PATH, args);
		marmot.executeAnalysis(anal);
		
		System.out.printf("output=%s, 소요시간=%ss%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void collectResult(MarmotRuntime marmot, String resultCsvPath, String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.print("단계: collect result -> ");
		
		String header = "X_COORD,Y_COORD,A_box_08,A_exp_08,A_pow_08,A_box_15,A_exp_15,A_pow_15";
		ParseCsvOptions opts = ParseCsvOptions.DEFAULT().header(header);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		
		Plan plan;
		plan = marmot.planBuilder("접근성 결과 수집")
					.loadTextFile(resultCsvPath)
					.parseCsv("text", opts)
					.filter("!X_COORD.equals('X_COORD')")
					.expand("A_box_08:double,A_exp_08:double,A_pow_08:double,A_box_15:double,A_exp_15:double,A_pow_15:double")
					.toPoint("X_COORD", "Y_COORD", "the_geom")
					.project("the_geom,*-{the_geom,X_COORD,Y_COORD}")
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%ss%n",
							ds.getId(), ds.getRecordCount(), watch.getElapsedMillisString());
	}
}
