package marmot.module;

import static marmot.optor.JoinOptions.INNER_JOIN;
import static marmot.optor.JoinOptions.RIGHT_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConcatPoliticals {
	private static final String SID = "구역/시도";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	private static final String LI = "구역/리";
	private static final String POLITICAL = "구역/통합법정동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		String merge = "if ( li_cd != null ) {"
				     + "	bjd_nm = emd_kor_nm + ' ' + li_kor_nm;"
				     + "    bjd_cd = li_cd;"
					 + "}"
					 + "else {"
					 + "	bjd_nm = emd_kor_nm;"
					 + "	bjd_cd = emd_cd + '00';"
					 + "	the_geom = emd_the_geom;"
					 + "}";
		
		String script1 = "if ( !sig_kor_nm.equals('세종특별자치시') ) {"
					   + "	bjd_nm = sig_kor_nm + ' ' + bjd_nm;"
					   + "}";

		DataSet ds = marmot.getDataSet(SID);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder("통합_행정경계구역_생성")
					.load(LI)
					.defineColumn("emd_cd2:string", "li_cd.substring(0,8)")
					.hashJoin("emd_cd2", EMD, "emd_cd",
							"*, param.the_geom as emd_the_geom,param.*-{the_geom}",
							RIGHT_OUTER_JOIN(1))
					.expand("bjd_nm:string,bjd_cd:string", merge)
					.project("the_geom,bjd_cd,bjd_nm,"
							+ "emd_cd,emd_kor_nm as emd_nm,"
							+ "li_cd,li_kor_nm as li_nm")
					.defineColumn("sig_cd2:string", "bjd_cd.substring(0,5)")
					.hashJoin("sig_cd2", SGG, "sig_cd", "*,param.sig_kor_nm", INNER_JOIN(1))
					.update(script1)
					.project("the_geom,bjd_cd,bjd_nm,"
							+ "sig_cd2 as sgg_cd,sig_kor_nm as sgg_nm,"
							+ "emd_cd,emd_nm,li_cd,li_nm")
					.defineColumn("sid_cd2:string", "bjd_cd.substring(0,2)")
					.hashJoin("sid_cd2", SID, "ctprvn_cd", "*,param.{ctp_kor_nm,ctprvn_cd}",
								INNER_JOIN(1))
					.update("bjd_nm = ctp_kor_nm + ' ' + bjd_nm")
					.project("the_geom,bjd_cd,bjd_nm,"
							+ "ctprvn_cd as sid_cd,ctp_kor_nm as sid_nm,"
							+ "sgg_cd,sgg_nm,emd_cd,emd_nm,li_cd,li_nm")
					.store(POLITICAL, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet output = marmot.getDataSet(POLITICAL);
		output.createSpatialIndex();
	}
}
