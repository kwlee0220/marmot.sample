package marmot.geom.advanced;

import common.SampleUtils;
import marmot.analysis.module.geo.arc.ArcUnionParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcUnion {
	private static final String INPUT = "안양대/공간연산/union/input";
	private static final String PARAM = "안양대/공간연산/union/param";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		ArcUnionParameters params = new ArcUnionParameters();
		params.setLeftDataSet(INPUT);
		params.setRightDataSet(PARAM);
		params.setOutputDataset(RESULT);
		params.setForce(true);
		
		marmot.executeProcess("arc_union", params.toMap());
		
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 5);
	}
}
