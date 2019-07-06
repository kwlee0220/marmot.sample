package marmot.geom;

import static marmot.StoreDataSetOptions.*;
import static marmot.optor.AggregateFunction.CONVEX_HULL;

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
public class SampleConvexHull {
	private static final String INPUT = "교통/버스/서울/정류소";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Plan plan = marmot.planBuilder("convex_hull")
								.load(INPUT)
								.aggregate(CONVEX_HULL("the_geom").as("the_geom"))
								.build();
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE(gcInfo));

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
