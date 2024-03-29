package marmot;

import static marmot.optor.StoreDataSetOptions.FORCE;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.plan.LoadOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadWholeFile {
	private static final String PATH = "data/기타/lfw";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		plan = Plan.builder("load_text")
					.loadWholeFile(PATH, LoadOptions.SPLIT_COUNT(16))
					.defineColumn("length:long", "length = bytes.length")
					.project("path,length")
					.store("tmp/result", FORCE)
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
	}
}
