package marmot.geom.advanced;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
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
		
		System.out.println(IDWInterpolation.ofPower(2));
		System.exit(1);

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		DataSet result;
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		
		String tempPath = "tmp/points";
		
		plan = Plan.builder("to_point")
						.load(INPUT)
						.centroid("the_geom")
						.project("the_geom, big_sq, value")
						.store(tempPath, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		result = marmot.getDataSet(tempPath);
		result.createSpatialIndex();
		
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
		plan = Plan.builder("calc_idw_interpolation")
					.load(tempPath)
					.interpolateSpatially("the_geom", tempPath, VALUE_COLUMN, RADIUS,
											"value", IDWInterpolation.ofPower(1))
					.store(RESULT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		result = marmot.getDataSet(RESULT);
		marmot.deleteDataSet(tempPath);
		
		SampleUtils.printPrefix(result, 5);
	}
}
