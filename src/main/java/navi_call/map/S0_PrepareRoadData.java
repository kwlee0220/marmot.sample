package navi_call.map;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClient;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;
import navi_call.Globals;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S0_PrepareRoadData {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();
		
		DataSet input = marmot.getDataSet(Globals.ROADS);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		String geomCol = gcInfo.name();
		
		Plan subPlan = marmot.planBuilder("서브 링크 순차번호 부여")
							.assignUid("sub_link_no")
							.build();
		
		Plan plan;
		plan = marmot.planBuilder("도로 링크 단순화")
					.load(Globals.ROADS)
					.flattenGeometry(geomCol, DataType.LINESTRING)
					.breakLineString(geomCol)
					.groupBy("link_id")
						.run(subPlan)
					.store(Globals.ROADS_IDX)
					.build();
		DataSet result = marmot.createDataSet(Globals.ROADS_IDX, plan, GEOMETRY(gcInfo), FORCE);
		System.out.printf("elapsed=%s (simplification)%n", watch.getElapsedMillisString());
		
		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}