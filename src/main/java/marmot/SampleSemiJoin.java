package marmot;

import static marmot.optor.JoinOptions.SEMI_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSemiJoin {
	private static final String INPUT = "구역/시군구";
	private static final String PARAM = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		Plan plan = Plan.builder("test semi_join")
							.load(INPUT)
							.hashJoin("sig_cd", PARAM, "sig_cd", "param.sig_kor_nm", SEMI_JOIN)
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 50);
	}
}
