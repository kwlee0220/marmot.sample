package marmot.validate;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CheckDataSetValidation {
	private static final String PREFIX = "tmp/validate/";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		Node SIDO = new SidoNode("sido", "구역/시도", "CTPRVN_CD", 2, PREFIX);
		Node SGG = new NonRootNode(SIDO, "sgg", "구역/시군구", "SIG_CD", 5, PREFIX);
		Node EMD = new NonRootNode(SGG, "emd", "구역/읍면동", "EMD_CD", 8, PREFIX);
		Node LI = new LiNode(EMD, "li", "구역/리", "LI_CD", 10, PREFIX);
		Node HJD = new NonRootNode(SGG, "hjd", "구역/행정동코드", "hcode", 10, PREFIX);
		Node BAS = new NonRootNode(SGG, "bas", "구역/기초구역", "bas_mgt_sn", 10, PREFIX);
		Node JJD = new NonRootNode(EMD, "jjd", "구역/연속지적도", "pnu", 19, PREFIX, 31);

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

//		SIDO.validate(marmot);
//		SGG.validate(marmot);
//		EMD.validate(marmot);
//		LI.validate(marmot);
//		HJD.validate(marmot);
//		BAS.validate(marmot);
		JJD.validate(marmot);
	}
}
