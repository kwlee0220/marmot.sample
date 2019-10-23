package demo.poc.e2sfca;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.exec.CompositeAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.optor.ParseCsvOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AddPublicAccess2 {
	private static final String ANALYSIS = "대중교통_접근성2";
	private static final String ANALY_COLLECT = "대중교통_접근성2/결과수집";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		marmot.deleteAnalysis(ANALYSIS, true);
	
		List<String> compIdList = new ArrayList<>();
		
		addCollectResult(marmot, compIdList);

		marmot.addAnalysis(new CompositeAnalysis(ANALYSIS, compIdList), true);
	}
	
	private static void addCollectResult(MarmotRuntime marmot, List<String> compIdList) {
		String header = "X_COORD,Y_COORD,A_box_08,A_exp_08,A_pow_08,A_box_15,A_exp_15,A_pow_15";
		ParseCsvOptions opts = ParseCsvOptions.DEFAULT().header(header);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		
		Plan plan;
		plan = marmot.planBuilder("접근성 결과 수집")
					.loadTextFile(Globals.CSV_RESULT_PATH)
					.parseCsv("text", opts)
					.filter("!X_COORD.equals('X_COORD')")
					.expand("A_box_08:double,A_exp_08:double,A_pow_08:double,A_box_15:double,A_exp_15:double,A_pow_15:double")
					.toPoint("X_COORD", "Y_COORD", "the_geom")
					.project("the_geom,*-{the_geom,X_COORD,Y_COORD}")
					.store(Globals.RESULT, StoreDataSetOptions.FORCE(gcInfo))
					.build();
		PlanAnalysis anal1 = new PlanAnalysis(ANALY_COLLECT, plan);
		marmot.addAnalysis(anal1, true);
		compIdList.add(anal1.getId());
	}
}
