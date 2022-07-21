package testcase.analysis;

import static marmot.optor.StoreDataSetOptions.FORCE;

import marmot.Plan;
import marmot.analysis.module.NormalizeParameters;
import marmot.analysis.system.SystemAnalysis;
import marmot.command.MarmotClientCommands;
import marmot.dataset.GeometryColumnInfo;
import marmot.exec.CompositeAnalysis;
import marmot.exec.ModuleAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleCompositeAnalysis {
	private static final String INPUT = "POI/주유소_가격";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		Plan plan;
		plan = Plan.builder("불필요한_컬럼_삭제")
					.load(INPUT)
					.project("the_geom,상호,휘발유")
					.store("tmp/reduced", FORCE(gcInfo))
					.build();
		marmot.addAnalysis(new PlanAnalysis("불필요한_컬럼_삭제", plan), true);
		
		SystemAnalysis analy = SystemAnalysis.clusterDataSet("색인_생성", "tmp/reduced");
		marmot.addAnalysis(analy, true);
		
		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset("tmp/reduced");
		params.outputDataset(RESULT);
		params.inputFeatureColumns("휘발유");
		params.outputFeatureColumns("normalized");
		ModuleAnalysis anal = new ModuleAnalysis("휘발유_표준화", "normalize", params.toMap());
		marmot.addAnalysis(anal, true);
		
		CompositeAnalysis composite
			= new CompositeAnalysis("복합작업", "불필요한_컬럼_삭제", "색인_생성", "휘발유_표준화");
		marmot.addAnalysis(composite, true);
	}
}
