package marmot.advanced;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAssignUid {
	private static final String INPUT = "주소/건물POI";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("sample_assign_uid")
							.load(INPUT)
							.filter("(long)출입구일련번호 % 119999 == 3")
							.assignUid("guid")
							.project("the_geom,guid,출입구일련번호")
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		SampleUtils.printPrefix(result, 5);
	}
}
