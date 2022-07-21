package marmot;

import static marmot.optor.StoreDataSetOptions.FORCE;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleHashJoin {
	private static final String INPUT = "구역/시군구";
	private static final String PARAM = "구역/시도";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		Plan plan = Plan.builder("test equi_join")
							.load(INPUT)
							.update("sig_cd = sig_cd.substring(0,2)")
							.hashJoin("sig_cd", PARAM, "ctprvn_cd",
									"the_geom,param.ctp_kor_nm,sig_kor_nm",
									JoinOptions.INNER_JOIN)
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		System.out.printf("should: %d == %d%n", result.getRecordCount(), input.getRecordCount());
		SampleUtils.printPrefix(result, 5);
	}
}
