package basic;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.ExecutePlanOption.DISABLE_LOCAL_EXEC;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClient;
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
		PBMarmotClient marmot = MarmotClient.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		Plan plan = marmot.planBuilder("sample_aggreate")
							.load(INPUT)
							.sort("보관일수:A:F,카메라대수:A")
							.project("the_geom,관리기관명,보관일수,카메라대수")
							.build();
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(RESULT, plan, DISABLE_LOCAL_EXEC,
												GEOMETRY(gcInfo), FORCE);
		SampleUtils.printPrefix(result, 5);
	}
}
