package marmot.geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.analysis.module.geo.arc.ArcMergeParameters;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcMerge {
	private static final String INPUT1 = "안양대/공간연산/merge/input";
	private static final String INPUT2 = "안양대/공간연산/merge/input2";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		ArcMergeParameters params = new ArcMergeParameters();
		params.setInputDatasets(INPUT1 + "," + INPUT2);
		params.setOutputDataset(RESULT);
		params.setForce(true);
		
		marmot.executeProcess("arc_merge", params.toMap());
		
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 5);
	}
}
