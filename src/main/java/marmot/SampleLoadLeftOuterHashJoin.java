package marmot;

import static marmot.ExecutePlanOptions.DISABLE_LOCAL_EXEC;
import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
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
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		DataSet result;
		
		DataSet input = marmot.getDataSet(SGG);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		plan = marmot.planBuilder("drop some sido")
					.load(SIDO)
					.filter("ctprvn_cd < 40")
					.build();
		marmot.createDataSet(TMP_SIDO, plan, FORCE(gcInfo));
		
		plan = marmot.planBuilder("drop some sgg")
					.load(SGG)
					.defineColumn("sido_cd:string", "sig_cd.substring(0,2)")
					.build();
		marmot.createDataSet(TMP_SGG, plan, FORCE(gcInfo));
		
		plan = marmot.planBuilder("test left_outer_equi_join")
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
