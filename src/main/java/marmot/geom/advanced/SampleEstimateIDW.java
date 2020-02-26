package marmot.geom.advanced;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;
import utils.func.FOption;

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
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		
		String tempPath = "tmp/points";
		
		plan = Plan.builder("to_point")
						.load(INPUT)
						.centroid("the_geom")
						.project("the_geom, big_sq, value")
						.store(RESULT, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		result = marmot.getDataSet(RESULT);
		result.createSpatialIndex();
		
		plan = Plan.builder("sample_estimate_idw")
						.load(tempPath)
						.estimateIdw("the_geom", tempPath, VALUE_COLUMN, RADIUS,
										TOP_K, "value", FOption.empty())
						.store(RESULT, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		result = marmot.getDataSet(RESULT);
		marmot.deleteDataSet(tempPath);
		
		SampleUtils.printPrefix(result, 5);
	}
}
