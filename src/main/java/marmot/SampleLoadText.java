package marmot;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadText {
	private static final String PATH = "data/로그/나비콜";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		plan = marmot.planBuilder("load_text")
					.loadTextFile(PATH)
					.filter("text.endsWith('37.633827')")
					.build();
		DataSet result = marmot.createDataSet("tmp/result", plan, DataSetOption.FORCE);
		SampleUtils.printPrefix(result, 5);
	}
}
