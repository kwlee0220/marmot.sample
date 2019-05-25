package marmot;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.optor.JoinOptions;
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
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("test semi_join")
							.load(INPUT)
							.hashJoin("sig_cd", PARAM, "sig_cd", "param.sig_kor_nm",
									JoinOptions.SEMI_JOIN())
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		SampleUtils.printPrefix(result, 50);
	}
}
