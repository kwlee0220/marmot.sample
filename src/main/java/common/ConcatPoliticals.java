package common;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.JoinOptions;
import marmot.optor.JoinType;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

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
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
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
		
		DataSet ds = marmot.getDataSet(SGG);

		Plan plan;
		plan = marmot.planBuilder("merge_politicals")
						.load(LI)
						.expand("emd_cd2:string", "emd_cd2 = li_cd.substring(0,8)")
						.join("emd_cd2", EMD, "emd_cd",
							"*, param.the_geom as emd_the_geom,param.*-{the_geom}",
							new JoinOptions().joinType(JoinType.RIGHT_OUTER_JOIN).workerCount(1))
						.expand("bjd_nm:string,bjd_cd:string", merge)
						.project("the_geom,bjd_cd,bjd_nm,"
								+ "emd_cd,emd_kor_nm as emd_nm,"
								+ "li_cd,li_kor_nm as li_nm")
						.expand("sig_cd2:string", "sig_cd2 = bjd_cd.substring(0,5)")
						.join("sig_cd2", SGG, "sig_cd", "*,param.sig_kor_nm",
								new JoinOptions().workerCount(1))
						.update(script1)
						.project("the_geom,bjd_cd,bjd_nm,"
								+ "sig_cd2 as sgg_cd,sig_kor_nm as sgg_nm,"
								+ "emd_cd,emd_nm,li_cd,li_nm")
						.expand("sid_cd2:string", "sid_cd2 = bjd_cd.substring(0,2)")
						.join("sid_cd2", SID, "ctprvn_cd", "*,param.{ctp_kor_nm,ctprvn_cd}",
								new JoinOptions().workerCount(1))
						.update("bjd_nm = ctp_kor_nm + ' ' + bjd_nm")
						.project("the_geom,bjd_cd,bjd_nm,"
								+ "ctprvn_cd as sid_cd,ctp_kor_nm as sid_nm,"
								+ "sgg_cd,sgg_nm,emd_cd,emd_nm,li_cd,li_nm")
						.store(POLITICAL)
						.build();
		DataSet result = marmot.createDataSet(POLITICAL, ds.getGeometryColumnInfo(), plan, true);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		result.cluster();
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
