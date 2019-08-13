package demo.dtg;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.geo.SquareGrid;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FilterOutInvalidDTG {
	private static final String REFERENCE = "구역/읍면동_2019";
	private static final String DTG = "교통/dtg_201809";
	private static final String TEMP_GRID = "tmp/grid/temp_grid";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet boundary = getValidBoundry(marmot);
		
		DataSet dtg = marmot.getDataSet(REFERENCE);
		GeometryColumnInfo gcInfo = dtg.getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("count invalid geometry records")
					.load(DTG)
					.spatialSemiJoin(gcInfo.name(), boundary.getId(), SpatialJoinOptions.NEGATED)
					.aggregate(COUNT())
					.build();
		long count = marmot.executeToLong(plan).get();
		watch.stop();
		
		System.out.printf("count=%d, total elapsed time=%s%n",
							count, watch.getElapsedMillisString());
	}
	
	private static DataSet getValidBoundry(PBMarmotClient marmot) {
		DataSet ds = marmot.getDataSet(REFERENCE);
		
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		Envelope bounds = ds.getBounds();
		SquareGrid grid = new SquareGrid(bounds, new Size2d(1000, 1000));
		
		String prjExpr = String.format("cell_geom as %s,cell_id,cell_pos", gcInfo.name());
		GeometryColumnInfo outGcInfo = new  GeometryColumnInfo(gcInfo.name(), "EPSG:4326");
		
		Plan plan;
		plan = marmot.planBuilder("build_square_grid")
					.load(REFERENCE)
					.assignGridCell(gcInfo.name(), grid, false)
					.project(prjExpr)
					.distinct("cell_id")
					// DTG 데이터가 방대하기 때문에, 구역 데이터의 좌표계를 DTG 좌표계인 EPSG:4326으로 변경시킨다.
					.transformCrs(outGcInfo.name(), gcInfo.srid(), outGcInfo.srid())
					.build();
		return marmot.createDataSet(TEMP_GRID, plan, StoreDataSetOptions.FORCE(outGcInfo));
	}
}
