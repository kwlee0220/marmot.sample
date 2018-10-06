package marmot.geom;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.plan.SpatialJoinOption.WITHIN_DISTANCE;

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
public class SampleSpatialSemiJoin {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "POI/주유소_가격";
	private static final String PARAMS = "교통/지하철/역사";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("within_distance")
							.load(INPUT)
							.spatialSemiJoin("the_geom", PARAMS, WITHIN_DISTANCE(30))
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
	}
}