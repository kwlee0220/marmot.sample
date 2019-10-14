package marmot;

import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Maps;

import marmot.command.MarmotClientCommands;
import marmot.exec.AnalysisNotFoundException;
import marmot.exec.CompositeAnalysis;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotAnalysis.Type;
import marmot.exec.ModuleAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.exec.SystemAnalysis;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSimpleAnalysis {
	private static final String INPUT = "주소/건물POI";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

//		marmot.deleteAnalysisAll();
		
		Plan plan;
		plan = marmot.planBuilder("test")
					.load(INPUT)
					.store(RESULT, StoreDataSetOptions.FORCE)
					.build();
		PlanAnalysis test1 = new PlanAnalysis("test1", plan);
		marmot.addAnalysis(test1);
		
		MarmotAnalysis anal;
		ModuleAnalysis module;
		CompositeAnalysis composite;
		
		anal = marmot.getAnalysis("test1");
		Utilities.checkState(anal.getId().equals("test1"));
		Utilities.checkState(anal.getType() == Type.PLAN);
		
		Map<String,String> margs = Maps.newHashMap();
		margs.put("arg1", "value1");
		margs.put("arg2", "value2");
		
		ModuleAnalysis test2 = new ModuleAnalysis("test2", "normalize", margs);
		marmot.addAnalysis(test2);
		module = (ModuleAnalysis)marmot.getAnalysis("test2");
		Utilities.checkState(module.getId().equals("test2"));
		Utilities.checkState(module.getType() == Type.MODULE);
		margs = module.getArguments();
		Utilities.checkState(margs.get("arg2").equals("value2"));
		
		CompositeAnalysis test5 = new CompositeAnalysis("test5", "test1", "test2");
		marmot.addAnalysis(test5);
		composite = (CompositeAnalysis)marmot.getAnalysis("test5");
		Utilities.checkState(composite.getId().equals("test5"));
		Utilities.checkState(composite.getComponents().size() == 2);
		Utilities.checkState(composite.getComponents().contains("test1"));
		Utilities.checkState(composite.getComponents().contains("test2"));
		
		List<String> idList = FStream.from(marmot.getDescendantAnalysisAll("test5"))
											.map(MarmotAnalysis::getId).toList();
		Utilities.checkState(idList.contains("test1"));
		Utilities.checkState(idList.contains("test2"));
		
		marmot.deleteAnalysis("test5", false);
		anal = marmot.getAnalysis("test1");
		anal = marmot.getAnalysis("test2");
		marmot.addAnalysis(test5);
		
		marmot.deleteAnalysis("test5", true);
		Utilities.checkState(marmot.findAnalysis("test1") == null);
		Utilities.checkState(marmot.findAnalysis("test2") == null);
		
		boolean failed = false;
		try {
			marmot.addAnalysis(test5);
		}
		catch ( AnalysisNotFoundException expected ) {
			failed = true;
		}
		Utilities.checkState(failed);

		marmot.addAnalysis(test1);
		marmot.addAnalysis(test2);
		marmot.addAnalysis(test5);
		
		SystemAnalysis test3 = SystemAnalysis.deleteDataSet("test3", "xxx");
		marmot.addAnalysis(test3);
		
		CompositeAnalysis test6 = new CompositeAnalysis("test6", "test5", "test3");
		marmot.addAnalysis(test6);
		anal = marmot.findParentAnalysis("test1");
		Utilities.checkState(anal.getId().equals("test5"));
		anal = marmot.findParentAnalysis("test5");
		Utilities.checkState(anal.getId().equals("test6"));
		
		idList = FStream.from(marmot.getDescendantAnalysisAll("test6"))
						.map(MarmotAnalysis::getId).toList();
		Utilities.checkState(idList.contains("test1"));
		Utilities.checkState(idList.contains("test2"));
		Utilities.checkState(idList.contains("test3"));
		Utilities.checkState(idList.contains("test5"));
		
		idList = FStream.from(marmot.getAncestorAnalysisAll("test2"))
						.map(MarmotAnalysis::getId).toList();
		Utilities.checkState(idList.contains("test5"));
		Utilities.checkState(idList.contains("test6"));
		
		idList = FStream.from(marmot.getAncestorAnalysisAll("test1"))
						.map(CompositeAnalysis::getId)
						.toList();
		Utilities.checkState(idList.size() == 2);
		Utilities.checkState(idList.contains("test5"));
		Utilities.checkState(idList.contains("test6"));

		idList = FStream.from(marmot.getAncestorAnalysisAll("test3"))
						.map(CompositeAnalysis::getId)
						.toList();
		Utilities.checkState(idList.size() == 1);
		Utilities.checkState(idList.contains("test6"));
		
		marmot.deleteAnalysis("test6", true);
		Utilities.checkState(marmot.findAnalysis("test1") == null);
		Utilities.checkState(marmot.findAnalysis("test2") == null);
		Utilities.checkState(marmot.findAnalysis("test3") == null);
		Utilities.checkState(marmot.findAnalysis("test5") == null);
		Utilities.checkState(marmot.findAnalysis("test6") == null);
	}
}
