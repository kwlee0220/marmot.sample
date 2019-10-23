package demo.poc.e2sfca;

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
public class AddPublicAccess1 {
	private static final String ANALYSIS = "대중교통_접근성";
	private static final String ANALY_FLOWPOP = "대중교통_접근성/강남구_유동인구";
	private static final String ANALY_BUS = "대중교통_접근성/강남구_버스";
	private static final String ANALY_SUBWAY = "대중교통_접근성/강남구_지하철";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		marmot.deleteAnalysis(ANALYSIS, true);
	
		List<String> compIdList = new ArrayList<>();
		
		addPopFlowGangnam(marmot, compIdList);
		addBusInfo(marmot, compIdList);
		addSubwayInfo(marmot, compIdList);

		marmot.addAnalysis(new CompositeAnalysis(ANALYSIS, compIdList), true);
	}
	
	private static void addPopFlowGangnam(MarmotRuntime marmot, List<String> compIdList) {
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("강남구_유동인구_추출")
						.load(Globals.FLOW_POP)
						.toXY("the_geom", "XCOORD", "YCOORD")
						.project("std_ym as STD_YM,block_cd as BLOCK_CD,"
								+ "XCOORD as X_COORD,YCOORD as Y_COORD,"
								+ "avg_08tmst as AVG_08TMST,avg_15tmst as AVG_15TMST,"
								+ "sum_area as SUM_area")
						.shard(1)
						.storeAsCsv(Globals.CSV_FLOWPOP_PATH, opts)
						.build();
		PlanAnalysis anal1 = new PlanAnalysis(ANALY_FLOWPOP, plan);
		marmot.addAnalysis(anal1, true);
		compIdList.add(anal1.getId());
	}
	
	private static void addBusInfo(MarmotRuntime marmot, List<String> compIdList) {
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("강남구_버스정보_추출")
					.load(Globals.BUS)
					.toXY("the_geom", "XCOORD", "YCOORD")
					.project("StationNM,XCOORD,YCOORD,ARSID,Slevel_08,Slevel_15")
					.shard(1)
					.storeAsCsv(Globals.CSV_BUS_PATH, opts)
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(ANALY_BUS, plan);
		marmot.addAnalysis(anal1, true);
		compIdList.add(anal1.getId());
	}
	
	private static void addSubwayInfo(MarmotRuntime marmot, List<String> compIdList) {
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT().headerFirst(true);
		
		Plan plan;
		plan = marmot.planBuilder("강남구_치하철정보_추출")
					.load(Globals.SUBWAY)
					.toXY("the_geom", "XCOORD", "YCOORD")
					.project("station_nm,XCOORD,YCOORD,line_num,Slevel_08,Slevel_15")
					.shard(1)
					.storeAsCsv(Globals.CSV_SUBWAY_PATH, opts)
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(ANALY_SUBWAY, plan);
		marmot.addAnalysis(anal1, true);
		compIdList.add(anal1.getId());
	}
	
	private static String OUTPUT(String analId) {
		return "/tmp/" + analId;
	}
}
