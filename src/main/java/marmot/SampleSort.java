package marmot;

import static marmot.ExecutePlanOptions.DISABLE_LOCAL_EXEC;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSort {
	private static final String INPUT = "POI/전국cctv";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("sample_aggreate")
							.load(INPUT)
							.sort("보관일수:A:F,카메라대수:A")
							.project("the_geom,관리기관명,보관일수,카메라대수")
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan, DISABLE_LOCAL_EXEC);

		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 5);
	}
}
