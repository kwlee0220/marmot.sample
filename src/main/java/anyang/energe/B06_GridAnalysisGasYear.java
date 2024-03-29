package anyang.energe;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.List;

import org.locationtech.jts.geom.Envelope;

import utils.Size2d;
import utils.StopWatch;
import utils.stream.FStream;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B06_GridAnalysisGasYear {
	private static final String INPUT = "tmp/anyang/map_gas" + Globals.YEAR;
	private static final String OUTPUT = "tmp/anyang/grid/grid_gas" + Globals.YEAR;
	
	private static final List<String> COL_NAMES = FStream.range(1, 13)
														.map(i -> "month_" + i)
														.toList();
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet cadastral = marmot.getDataSet(INPUT);
		Envelope bounds = cadastral.getBounds();
		Size2d cellSize = new Size2d(1000, 1000);
		
		String updateExpr = FStream.from(COL_NAMES)
									.map(c -> String.format("%s *= ratio", c))
									.join("; ");
		List<AggregateFunction> aggrs = FStream.from(COL_NAMES)
												.map(c -> SUM(c).as(c))
												.toList();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		
		Plan plan;
		String planName = String.format("%d 가스 사용량 격자 분석", Globals.YEAR);
		plan = Plan.builder(planName)
					.load(INPUT)
					.assignGridCell("the_geom", new SquareGrid(bounds, cellSize), false)
					.intersection("the_geom", "cell_geom", "overlap")
					.defineColumn("ratio:double", "(ST_Area(overlap) /  ST_Area(the_geom))")
					.update(updateExpr)
					.aggregateByGroup(Group.ofKeys("cell_id").withTags("cell_geom,cell_pos")
											.workerCount(7),
										aggrs)
					.expand("x:long,y:long", "x = cell_pos.getX(); y = cell_pos.getY()")
					.project("cell_geom as the_geom, x, y, *-{cell_geom,x,y}")
					.store(OUTPUT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		for ( int month = 1; month <= 12; ++month ) {
			extractToMonth(marmot, month);
		}
		marmot.deleteDataSet(OUTPUT);
		marmot.close();
		
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
	}
	
	private static void extractToMonth(PBMarmotClient marmot, int month) {
		String output = String.format("%s_splits/%d", OUTPUT, month);
		String projectExpr = String.format("the_geom,x,y,month_%d as value", month);
		
		DataSet ds = marmot.getDataSet(OUTPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder("월별 격자 분석 추출")
					.load(OUTPUT)
					.project(projectExpr)
					.store(output, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
	}
}
