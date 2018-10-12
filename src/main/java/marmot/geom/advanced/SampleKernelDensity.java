package marmot.geom.advanced;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.geo.SquareGrid;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Size2d;

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
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeometryColumnInfo info = new GeometryColumnInfo("the_geom", "EPSG:5186");
		SquareGrid grid = new SquareGrid(INPUT, new Size2d(1000, 1000));
		Plan plan = marmot.planBuilder("sample_estimate_kernel_density")
						.loadSquareGridFile(grid, -1)
						.estimateKernelDensity("the_geom", INPUT, VALUE_COLUMN, RADIUS, VALUE_COLUMN)
						.build();
		DataSet result = marmot.createDataSet(RESULT, plan, GEOMETRY(info), FORCE);
		SampleUtils.printPrefix(result, 5);
	}
}
