package demo.poc.map;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.exec.CompositeAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.optor.StoreAsCsvOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AddTaxiRoad {
	private static final String TAXI_LOG = "연세대/사업단실증/MapMatching/택시로그";
	
	private static final String ANALYSIS = "택시승하차_분석";
	private static final String ANALY_TAXI_LOG = "택시승하차_분석/택시승하차";
	private static final String ANALY_BUS = "대중교통_접근성/강남구_버스";
	private static final String ANALY_SUBWAY = "대중교통_접근성/강남구_지하철";
	
	private static final String CSV_TAXI_LOG_PATH = "tmp/taxi/MapMatching_input.csv";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		marmot.deleteAnalysis(ANALYSIS, true);
	
		List<String> compIdList = new ArrayList<>();
		
		addTaxiLog(marmot, compIdList);
//		addBusInfo(marmot, compIdList);
//		addSubwayInfo(marmot, compIdList);

		marmot.addAnalysis(new CompositeAnalysis(ANALYSIS, compIdList), true);
	}
	
	private static void addTaxiLog(MarmotRuntime marmot, List<String> compIdList) {
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("택시_승하차_추출")
						.load(TAXI_LOG)
						.expand("status:int")
						.filter("status==1 || status == 2")
						.toXY("the_geom", "x_wgs84", "y_wgs84")
						.project("vehicle,date,month_created,area_code,x_bessel,y_bessel,status,company_code,driver_ID,x_wgs84,y_wgs84")
						.shard(1)
						.storeAsCsv(CSV_TAXI_LOG_PATH, opts)
						.build();
		PlanAnalysis anal1 = new PlanAnalysis(ANALY_TAXI_LOG, plan);
		marmot.addAnalysis(anal1, true);
		compIdList.add(anal1.getId());
	}
	
//	private static void addCollectMatching(MarmotRuntime marmot, List<String> compIdList) {
//		String header = "X_COORD,Y_COORD,A_box_08,A_exp_08,A_pow_08,A_box_15,A_exp_15,A_pow_15";
//		ParseCsvOptions opts = ParseCsvOptions.DEFAULT().header(header);
//		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
//		
//		Plan plan;
//		plan = marmot.planBuilder("접근성 결과 수집")
//					.loadTextFile(Globals.CSV_RESULT_PATH)
//					.parseCsv("text", opts)
//					.filter("!X_COORD.equals('X_COORD')")
//					.expand("A_box_08:double,A_exp_08:double,A_pow_08:double,A_box_15:double,A_exp_15:double,A_pow_15:double")
//					.toPoint("X_COORD", "Y_COORD", "the_geom")
//					.project("the_geom,*-{the_geom,X_COORD,Y_COORD}")
//					.store(Globals.RESULT, StoreDataSetOptions.FORCE(gcInfo))
//					.build();
//		PlanAnalysis anal1 = new PlanAnalysis(ANALY_COLLECT, plan);
//		marmot.addAnalysis(anal1, true);
//		compIdList.add(anal1.getId());
//	}
	
//	private static void addTaxiLog(MarmotRuntime marmot, List<String> compIdList) {
//		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
//		
//		Plan plan;
//		plan = marmot.planBuilder("강남구_버스정보_추출")
//					.load(Globals.BUS)
//					.toXY("the_geom", "XCOORD", "YCOORD")
//					.project("StationNM,XCOORD,YCOORD,ARSID,Slevel_08,Slevel_15")
//					.shard(1)
//					.storeAsCsv(Globals.CSV_BUS_PATH, opts)
//					.build();
//		PlanAnalysis anal1 = new PlanAnalysis(ANALY_BUS, plan);
//		marmot.addAnalysis(anal1, true);
//		compIdList.add(anal1.getId());
//	}
//	
//	private static void addSubwayInfo(MarmotRuntime marmot, List<String> compIdList) {
//		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
//		
//		Plan plan;
//		plan = marmot.planBuilder("강남구_치하철정보_추출")
//					.load(Globals.SUBWAY)
//					.toXY("the_geom", "XCOORD", "YCOORD")
//					.project("station_nm,XCOORD,YCOORD,line_num,Slevel_08,Slevel_15")
//					.shard(1)
//					.storeAsCsv(Globals.CSV_SUBWAY_PATH, opts)
//					.build();
//		PlanAnalysis anal1 = new PlanAnalysis(ANALY_SUBWAY, plan);
//		marmot.addAnalysis(anal1, true);
//		compIdList.add(anal1.getId());
//	}
//	
//	private static String OUTPUT(String analId) {
//		return "/tmp/" + analId;
//	}
}
