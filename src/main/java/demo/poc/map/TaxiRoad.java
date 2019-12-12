package demo.poc.map;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.exec.ExternAnalysis;
import marmot.optor.JoinOptions;
import marmot.optor.ParseCsvOptions;
import marmot.optor.StoreAsCsvOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TaxiRoad {
	private static final String TAXI_LOG = "연세대/사업단실증/MapMatching/택시로그";
	private static final String ROAD = "연세대/사업단실증/MapMatching/도로_링크";
	private static final String ROAD_MESH = "연세대/사업단실증/MapMatching/link_mesh";
	private static final String RESULT = "분석결과/도로별_승하차_순위";
	
	private static final String ANALY_MAP_MATCHING = "택시승하차_분석/맵_매칭";
	private static final String ANALY_RANK = "택시승하차_분석/도로별_승하차_순위선정";

	private static final String CSV_LINK_MESH_PATH = "tmp/taxi/link_mesh.csv";
	private static final String CSV_TAXI_LOG_PATH = "tmp/taxi/MapMatching_input.csv";
	private static final String CSV_MAP_MATCHING_PATH = "tmp/taxi/MapMatching_output.csv";
	private static final String CSV_RANK_INPUT_PATH = "tmp/taxi/rank_input.csv";
	private static final String CSV_RANK_OUTPUT_PATH = "tmp/taxi/rank_output.csv";
	
	private static final String SPARK_PATH = "/usr/bin/spark-submit";
	private static final String HEADER = "vehicle,date,month_created,area_code,x_bessel,y_bessel,status,company_code,driver_ID,x_wgs84,y_wgs84";
	private static final String HEADER2 = HEADER + ",mesh,link_id,car2LinkDistance";
	private static final String RANK_HEADER = "WKT,Join_Count,Id,X,Y,Percentile_Rank";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
		StopWatch watch = StopWatch.start();
		System.out.println("시작: 택시 승하차 분석 (MapMatching & PercentileRank)...... ");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		setTaxiLog(marmot, CSV_TAXI_LOG_PATH);
		setLinkMesh(marmot, CSV_LINK_MESH_PATH);
		execMapMatching(marmot, CSV_TAXI_LOG_PATH, CSV_LINK_MESH_PATH, CSV_MAP_MATCHING_PATH);
		countRoadLinks(marmot, CSV_RANK_INPUT_PATH);
		execRank(marmot, CSV_RANK_INPUT_PATH, CSV_RANK_OUTPUT_PATH);
		collectRank(marmot, CSV_RANK_OUTPUT_PATH, RESULT);

		DataSet ds = marmot.getDataSet(RESULT);
		System.out.printf("종료: %s(%d건), 소요시간=%s%n",
				ds.getId(), ds.getRecordCount(), watch.getElapsedMillisString());
	}
	
	private static void setTaxiLog(MarmotRuntime marmot, String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("\t단계: 오전_9시_택시_승차로그_추출 준비 -> "); System.out.flush();
		
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("오전_9시_택시_하차_추출")
					.load(TAXI_LOG)
					.filter("status == '2' && date.substring(8,10) == '09'")
					.sample(0.05)
					.project(HEADER)
					.shard(1)
					.storeAsCsv(outCsvPath, opts)
					.build();
		marmot.execute(plan);
		
		System.out.printf("output=%s, 소요시간=%s%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void setLinkMesh(MarmotRuntime marmot, String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("\t단계: 도로링크 메쉬 데이터 준비 -> "); System.out.flush();
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("도로_링크_메쉬_추출")
					.load(ROAD_MESH)
					.shard(1)
					.storeAsCsv(outCsvPath, opts)
					.build();
		marmot.execute(plan);
		
		System.out.printf("output=%s, 소요시간=%s%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void execMapMatching(MarmotRuntime marmot, String taxiPath, String linkPath,
										String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("\t단계: MapMatching -> "); System.out.flush();
		
		String[] args = new String[] {
			"--packages", "org.datasyslab:geospark:1.2.0,org.datasyslab:geospark-sql_2.3:1.2.0",
			"--class", "main.scala.MapMatching", "extensions/mapmatching_2.11-yarn_3.0.jar",
			taxiPath, linkPath, outCsvPath, HEADER
		};

		ExternAnalysis anal = new ExternAnalysis(ANALY_MAP_MATCHING, SPARK_PATH, args);
		marmot.executeAnalysis(anal);
		
		System.out.printf("output=%s, 소요시간=%s%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void countRoadLinks(MarmotRuntime marmot, String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("\t단계: 도로링크별 택시 승하차 횟수 누적 -> "); System.out.flush();
		
		ParseCsvOptions opts = ParseCsvOptions.DEFAULT().header(HEADER2);
		StoreAsCsvOptions storeOpts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("맵매칭 결과 수집")
					.loadTextFile(CSV_MAP_MATCHING_PATH)
					.parseCsv("text", opts)
					.filter("!vehicle.equals('vehicle')")
					.project("link_id")
					.aggregateByGroup(Group.ofKeys("link_id"), COUNT().as("Join_Count"))
					.defineColumn("WKT:string", "'N/A'")
					.defineColumn("X:double", "0")
					.defineColumn("Y:double", "0")
					.project("WKT,Join_Count,link_id as Id,X,Y")
					.storeAsCsv(outCsvPath, storeOpts)
					.build();
		marmot.execute(plan);
		
		System.out.printf("output=%s, 소요시간=%s%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void execRank(MarmotRuntime marmot, String rankInput, String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("\t단계: PercentileRank -> "); System.out.flush();
		
		String[] args = new String[] {
			"--class", "main.scala.PercentileRank",
			"extensions/percentilerank_2.11-yarn_3.0.jar",
			rankInput, outCsvPath,
			"WKT,Join_Count,Id,X,Y"
		};

		ExternAnalysis anal = new ExternAnalysis(ANALY_RANK, SPARK_PATH, args);
		marmot.executeAnalysis(anal);
		
		System.out.printf("output=%s, 소요시간=%s%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void collectRank(MarmotRuntime marmot, String resultCsvPath, String outDsId) {
		StopWatch watch = StopWatch.start();
		System.out.print("\t단계: 순위 결과 수집 -> "); System.out.flush();
		
		ParseCsvOptions opts = ParseCsvOptions.DEFAULT().header(RANK_HEADER);
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(ROAD).getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder("순위 결과 수집")
					.loadTextFile(resultCsvPath)
					.parseCsv("text", opts)
					.filter("!WKT.equals('WKT')")
					.defineColumn("Percentile_Rank:float")
					.project("id as link_id,Percentile_Rank as rank")
					.hashJoin("link_id", ROAD, "link_id", "param.the_geom,link_id,rank",
								JoinOptions.INNER_JOIN)
					.store(outDsId, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet ds = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%s%n",
							ds.getId(), ds.getRecordCount(), watch.getElapsedMillisString());
	}
}
