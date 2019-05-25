package marmot.geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.geo.advanced.IDWInterpolation;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleInterpolateSpatially {
	private static final String RESULT = "tmp/result2";
	private static final String INPUT = "주민/인구밀도_2000";
	private static final String VALUE_COLUMN = "value";
	private static final double RADIUS = 3 * 1000;
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
		result = marmot.createDataSet(tempPath, plan, StoreDataSetOptions.create().geometryColumnInfo(info).force(true));
		result.cluster();
		
		String interpolator = "def interpolate(factors) { "
								+ "double numerator = 0;"
								+ "double denominator = 0;"
								+ "foreach (p: factors) {"
									+ "double weight = Math.pow(p.distance, 2);"
									+ "weight = (weight == 0) ? 1 : 1/weight;"
									+ "numerator += (p.measure * weight);"
									+ "denominator += weight;"
								+ "}"
								+ "return numerator / denominator;}";
		plan = marmot.planBuilder("calc_idw_interpolation")
					.load(tempPath)
					.interpolateSpatially("the_geom", tempPath, VALUE_COLUMN, RADIUS,
											"value", IDWInterpolation.ofPower(1))
					.build();
		result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().geometryColumnInfo(info).force(true));
		marmot.deleteDataSet(tempPath);
		
		SampleUtils.printPrefix(result, 5);
	}
}
