package marmot.analysis;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import utils.Preconditions;
import utils.stream.FStream;

import marmot.Plan;
import marmot.analysis.system.SystemAnalysis;
import marmot.command.MarmotClientCommands;
import marmot.exec.AnalysisNotFoundException;
import marmot.exec.CompositeAnalysis;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotAnalysis.Type;
import marmot.exec.ModuleAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.optor.StoreDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSimpleAnalysis {
	private static final String INPUT = "주소/건물POI";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

//		marmot.deleteAnalysisAll();
		
		Plan plan;
		plan = Plan.builder("test")
					.load(INPUT)
					.store(RESULT, StoreDataSetOptions.FORCE)
					.build();
		PlanAnalysis test1 = new PlanAnalysis("test1", plan);
		marmot.addAnalysis(test1, true);
		
		MarmotAnalysis anal;
		ModuleAnalysis module;
		CompositeAnalysis composite;
		
		anal = marmot.getAnalysis("test1");
		Preconditions.checkState(anal.getId().equals("test1"));
		Preconditions.checkState(anal.getType() == Type.PLAN);
		
		Map<String,String> margs = Maps.newHashMap();
		margs.put("arg1", "value1");
		margs.put("arg2", "value2");
		
		ModuleAnalysis test2 = new ModuleAnalysis("test2", "normalize", margs);
		marmot.addAnalysis(test2, true);
		module = (ModuleAnalysis)marmot.getAnalysis("test2");
		Preconditions.checkState(module.getId().equals("test2"));
		Preconditions.checkState(module.getType() == Type.MODULE);
		margs = module.getArguments();
		Preconditions.checkState(margs.get("arg2").equals("value2"));
		
		CompositeAnalysis test5 = new CompositeAnalysis("test5", "test1", "test2");
		marmot.addAnalysis(test5, true);
		composite = (CompositeAnalysis)marmot.getAnalysis("test5");
		Preconditions.checkState(composite.getId().equals("test5"));
		Preconditions.checkState(composite.getComponents().size() == 2);
		Preconditions.checkState(composite.getComponents().contains("test1"));
		Preconditions.checkState(composite.getComponents().contains("test2"));
		
		List<String> idList = FStream.from(marmot.getDescendantAnalysisAll("test5"))
											.map(MarmotAnalysis::getId).toList();
		Preconditions.checkState(idList.contains("test1"));
		Preconditions.checkState(idList.contains("test2"));
		
		marmot.deleteAnalysis("test5", false);
		anal = marmot.getAnalysis("test1");
		anal = marmot.getAnalysis("test2");
		marmot.addAnalysis(test5, true);
		
		marmot.deleteAnalysis("test5", true);
		Preconditions.checkState(marmot.findAnalysis("test1") == null);
		Preconditions.checkState(marmot.findAnalysis("test2") == null);
		
		boolean failed = false;
		try {
			marmot.addAnalysis(test5, true);
		}
		catch ( AnalysisNotFoundException expected ) {
			failed = true;
		}
		Preconditions.checkState(failed);

		marmot.addAnalysis(test1, true);
		marmot.addAnalysis(test2, true);
		marmot.addAnalysis(test5, true);
		
		SystemAnalysis test3 = SystemAnalysis.deleteDataSet("test3", "xxx");
		marmot.addAnalysis(test3, true);
		
		CompositeAnalysis test6 = new CompositeAnalysis("test6", "test5", "test3");
		marmot.addAnalysis(test6, true);
		anal = marmot.findParentAnalysis("test1");
		Preconditions.checkState(anal.getId().equals("test5"));
		anal = marmot.findParentAnalysis("test5");
		Preconditions.checkState(anal.getId().equals("test6"));
		
		idList = FStream.from(marmot.getDescendantAnalysisAll("test6"))
						.map(MarmotAnalysis::getId).toList();
		Preconditions.checkState(idList.contains("test1"));
		Preconditions.checkState(idList.contains("test2"));
		Preconditions.checkState(idList.contains("test3"));
		Preconditions.checkState(idList.contains("test5"));
		
		idList = FStream.from(marmot.getAncestorAnalysisAll("test2"))
						.map(MarmotAnalysis::getId).toList();
		Preconditions.checkState(idList.contains("test5"));
		Preconditions.checkState(idList.contains("test6"));
		
		idList = FStream.from(marmot.getAncestorAnalysisAll("test1"))
						.map(CompositeAnalysis::getId)
						.toList();
		Preconditions.checkState(idList.size() == 2);
		Preconditions.checkState(idList.contains("test5"));
		Preconditions.checkState(idList.contains("test6"));

		idList = FStream.from(marmot.getAncestorAnalysisAll("test3"))
						.map(CompositeAnalysis::getId)
						.toList();
		Preconditions.checkState(idList.size() == 1);
		Preconditions.checkState(idList.contains("test6"));
		
		marmot.deleteAnalysis("test6", true);
		Preconditions.checkState(marmot.findAnalysis("test1") == null);
		Preconditions.checkState(marmot.findAnalysis("test2") == null);
		Preconditions.checkState(marmot.findAnalysis("test3") == null);
		Preconditions.checkState(marmot.findAnalysis("test5") == null);
		Preconditions.checkState(marmot.findAnalysis("test6") == null);
	}
}
