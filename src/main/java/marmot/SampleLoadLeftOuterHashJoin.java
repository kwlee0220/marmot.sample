package marmot;

import static marmot.ExecutePlanOptions.DISABLE_LOCAL_EXEC;
import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;
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
public class SampleLoadLeftOuterHashJoin {
	private static final String SGG = "구역/시군구";
	private static final String SIDO = "구역/시도";
	private static final String TMP_SGG = "tmp/sgg";
	private static final String TMP_SIDO = "tmp/sido";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		DataSet result;
		
		DataSet input = marmot.getDataSet(SGG);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		plan = Plan.builder("drop some sido")
					.load(SIDO)
					.filter("ctprvn_cd < 40")
					.store(TMP_SIDO, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		plan = Plan.builder("drop some sgg")
					.load(SGG)
					.defineColumn("sido_cd:string", "sig_cd.substring(0,2)")
					.store(TMP_SGG, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		plan = Plan.builder("test left_outer_equi_join")
					.loadHashJoin(TMP_SGG, "sido_cd", TMP_SIDO, "ctprvn_cd",
									"left.the_geom,right.ctp_kor_nm,left.sig_kor_nm,left.sig_cd",
									LEFT_OUTER_JOIN)
//					.sample(0.2)
					.store(RESULT, FORCE(gcInfo))
					.build();
		marmot.execute(plan, DISABLE_LOCAL_EXEC);

		result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 500);
		
//		marmot.deleteDataSet(TMP_SGG);
//		marmot.deleteDataSet(TMP_SIDO);
	}
}
