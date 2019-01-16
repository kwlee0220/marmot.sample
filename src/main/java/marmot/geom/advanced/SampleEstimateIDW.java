package marmot.geom.advanced;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleEstimateIDW {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "주민/인구밀도_2000";
	private static final String VALUE_COLUMN = "value";
	private static final double RADIUS = 5 * 1000;
	private static final int TOP_K = 10;

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		DataSet result;
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:5186");
		
		String tempPath = "tmp/points";
		
		plan = marmot.planBuilder("to_point")
						.load(INPUT)
						.centroid("the_geom")
						.project("the_geom, big_sq, value")
						.build();
		result = marmot.createDataSet(tempPath, plan, GEOMETRY(info), FORCE);
		result.cluster();
		
		plan = marmot.planBuilder("sample_estimate_idw")
						.load(tempPath)
						.estimateIDW("the_geom", tempPath, VALUE_COLUMN, RADIUS,
										TOP_K, "value")
						.build();
		result = marmot.createDataSet(RESULT, plan, GEOMETRY(info), FORCE);
		marmot.deleteDataSet(tempPath);
		
		SampleUtils.printPrefix(result, 5);
	}
}
