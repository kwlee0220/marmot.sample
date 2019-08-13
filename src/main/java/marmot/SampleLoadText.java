package marmot;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadText {
	private static final String PATH = "data/로그/dtg_201809";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		plan = marmot.planBuilder("load_text")
					.loadTextFile(PATH)
					.aggregate(AggregateFunction.COUNT())
//					.filter("text.endsWith('37.633827')")
					.build();
		DataSet result = marmot.createDataSet("tmp/result", plan, StoreDataSetOptions.FORCE);
		SampleUtils.printPrefix(result, 5);
	}
}
