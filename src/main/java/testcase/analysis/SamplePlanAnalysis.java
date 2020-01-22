package testcase.analysis;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.GeometryColumnInfo;
import marmot.exec.PlanAnalysis;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SamplePlanAnalysis {
	private static final String INPUT = "POI/주유소_가격";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder("컬럼_제거")
					.load(INPUT)
					.project("the_geom,상호,휘발유")
					.store(RESULT, FORCE(gcInfo))
					.build();
		marmot.addAnalysis(new PlanAnalysis("컬럼_제거", plan), true);
	}
}
