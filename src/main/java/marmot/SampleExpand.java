package marmot;

import static marmot.StoreDataSetOptions.*;
import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExpand {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("update")
							.load(INPUT)
							.expand("the_geom:point,area:double",
									"area = ST_Area(the_geom);"
										+ "the_geom = ST_Centroid(the_geom);"
										+ "kor_SUB_nm = 'Station(' + kor_sub_nm + ')';"
										+ "eng_sub_nm = kor_sub_nm + '_ENG'")
							.expand("sig_cd:int")
							.project("the_geom,area,SIG_CD,kor_sub_nm")
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE(gcInfo));
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
