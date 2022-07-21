package demo.poc.map;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.ArrayList;
import java.util.List;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.GeometryColumnInfo;
import marmot.exec.CompositeAnalysis;
import marmot.exec.ExternAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.optor.JoinOptions;
import marmot.optor.ParseCsvOptions;
import marmot.optor.StoreAsCsvOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AddTaxiRoad {
	private static final String TAXI_LOG = "연세대/사업단실증/MapMatching/택시로그";
	private static final String ROAD = "연세대/사업단실증/MapMatching/도로_링크";
	private static final String ROAD_MESH = "연세대/사업단실증/MapMatching/link_mesh";
	private static final String RESULT = "분석결과/도로별_승하차_순위";
	
	private static final String ANALYSIS = "택시승하차_분석";
	private static final String ANALY_TAXI_LOG = "택시승하차_분석/오전_9시_택시_하차_추출";
	private static final String ANALY_LINK_MESH = "택시승하차_분석/도로_링크_메쉬_준비";
	private static final String ANALY_MAP_MATCHING = "택시승하차_분석/맵_매칭";
	private static final String ANALY_ROAD_COUNT = "택시승하차_분석/도로별_승하차_합계";
	private static final String ANALY_RANK = "택시승하차_분석/도로별_승하차_순위선정";
	private static final String ANALY_ROAD_RANK = "택시승하차_분석/도로별_승하차_순위_수집";

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
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		marmot.deleteAnalysis(ANALYSIS, true);
	
		List<String> compIdList = new ArrayList<>();
		
		addTaxiLog(marmot, compIdList);
		addLinkMesh(marmot, compIdList);
		addExecMapMatching(marmot, compIdList);
		addRoadCount(marmot, compIdList);
		addExecRank(marmot, compIdList);
		addCollectRank(marmot, compIdList);

		marmot.addAnalysis(new CompositeAnalysis(ANALYSIS, compIdList), true);
	}
	
	private static void addTaxiLog(MarmotRuntime marmot, List<String> compIdList) {
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = Plan.builder("오전_9시_택시_하차_추출")
					.load(TAXI_LOG)
					.filter("status == '2' && date.substring(8,10) == '09'")
					.sample(0.05)
					.project(HEADER)
					.shard(1)
					.storeAsCsv(CSV_TAXI_LOG_PATH, opts)
					.build();
		PlanAnalysis anal = new PlanAnalysis(ANALY_TAXI_LOG, plan);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());
	}
	
	private static void addLinkMesh(MarmotRuntime marmot, List<String> compIdList) {
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = Plan.builder("도로_링크_메쉬_추출")
					.load(ROAD_MESH)
					.shard(1)
					.storeAsCsv(CSV_LINK_MESH_PATH, opts)
					.build();
		PlanAnalysis anal = new PlanAnalysis(ANALY_LINK_MESH, plan);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());
	}
	
	private static void addExecMapMatching(MarmotRuntime marmot, List<String> compIdList) {
		String[] args = new String[] {
			"--packages", "org.datasyslab:geospark:1.2.0,org.datasyslab:geospark-sql_2.3:1.2.0",
			"--class", "main.scala.MapMatching", "extensions/mapmatching_2.11-yarn_3.0.jar",
			CSV_TAXI_LOG_PATH, CSV_LINK_MESH_PATH, CSV_MAP_MATCHING_PATH,
			HEADER
		};
		
		ExternAnalysis anal = new ExternAnalysis(ANALY_MAP_MATCHING, SPARK_PATH, args);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());
	}
	
	private static void addRoadCount(MarmotRuntime marmot, List<String> compIdList) {
		ParseCsvOptions opts = ParseCsvOptions.DEFAULT().header(HEADER2);
		StoreAsCsvOptions storeOpts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = Plan.builder("맵매칭 결과 수집")
					.loadTextFile(CSV_MAP_MATCHING_PATH)
					.parseCsv("text", opts)
					.filter("!vehicle.equals('vehicle')")
					.project("link_id")
					.aggregateByGroup(Group.ofKeys("link_id"), COUNT().as("Join_Count"))
					.defineColumn("WKT:string", "'N/A'")
					.defineColumn("X:double", "0")
					.defineColumn("Y:double", "0")
					.project("WKT,Join_Count,link_id as Id,X,Y")
					.storeAsCsv(CSV_RANK_INPUT_PATH, storeOpts)
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(ANALY_ROAD_COUNT, plan);
		marmot.addAnalysis(anal1, true);
		compIdList.add(anal1.getId());
	}
	
	private static void addExecRank(MarmotRuntime marmot, List<String> compIdList) {
		String[] args = new String[] {
			"--class", "main.scala.PercentileRank",
			"extensions/percentilerank_2.11-yarn_3.0.jar",
			CSV_RANK_INPUT_PATH, CSV_RANK_OUTPUT_PATH,
			"WKT,Join_Count,Id,X,Y"
		};
		
		ExternAnalysis anal = new ExternAnalysis(ANALY_RANK, SPARK_PATH, args);
		marmot.addAnalysis(anal, true);
		compIdList.add(anal.getId());
	}
	
	private static void addCollectRank(MarmotRuntime marmot, List<String> compIdList) {
		ParseCsvOptions opts = ParseCsvOptions.DEFAULT().header(RANK_HEADER);
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(ROAD).getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder("순위 결과 수집")
					.loadTextFile(CSV_RANK_OUTPUT_PATH)
					.parseCsv("text", opts)
					.filter("!WKT.equals('WKT')")
					.defineColumn("Percentile_Rank:float")
					.project("id as link_id,Percentile_Rank as rank")
					.hashJoin("link_id", ROAD, "link_id", "param.the_geom,link_id,rank",
							JoinOptions.INNER_JOIN)
					.store(RESULT, FORCE(gcInfo))
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(ANALY_ROAD_RANK, plan);
		marmot.addAnalysis(anal1, true);
		compIdList.add(anal1.getId());
	}
}
