package marmot.geom.advanced;

import common.SampleUtils;
import marmot.analysis.module.geo.arc.ArcSplitParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcSplit {
	private static final String INPUT = "안양대/공간연산/split/input";
	private static final String PARAM = "안양대/공간연산/split/param";
	private static final String RESULT = "tmp/result_splits";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		ArcSplitParameters params = new ArcSplitParameters();
		params.setInputDataset(INPUT);
		params.setSplitDataset(PARAM);
		params.setSplitKey("EMD_KOR_NM");
		params.setOutputDataset(RESULT);
		params.setForce(true);
		marmot.executeProcess("arc_split", params.toMap());
		
		for ( DataSet ds: marmot.getDataSetAllInDir(RESULT, true) ) {
			System.out.println(ds.getId() + ":");
			
			// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
			SampleUtils.printPrefix(ds, 5);
		}
		
	}
}
