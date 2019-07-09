package marmot;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.plan.LoadOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadWholeFile {
	private static final String PATH = "data/기타/lfw";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		plan = marmot.planBuilder("load_text")
					.loadWholeFile(PATH, LoadOptions.SPLIT_COUNT(16))
					.defineColumn("length:long", "length = bytes.length")
					.project("path,length")
					.build();
		DataSet result = marmot.createDataSet("tmp/result", plan, StoreDataSetOptions.FORCE);
		SampleUtils.printPrefix(result, 5);
	}
}
