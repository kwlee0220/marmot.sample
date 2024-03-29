package marmot.geom.advanced;

import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.Size2d;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.SquareGrid;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleKernelDensity {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "주민/인구밀도_2000";
	private static final String VALUE_COLUMN = "value";
	private static final double RADIUS = 1 * 1000;

	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		SquareGrid grid = new SquareGrid(INPUT, new Size2d(1000, 1000));
		Plan plan = Plan.builder("sample_estimate_kernel_density")
						.loadGrid(grid)
						.estimateKernelDensity("the_geom", INPUT, VALUE_COLUMN, RADIUS, VALUE_COLUMN)
						.store(RESULT, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 5);
	}
}
