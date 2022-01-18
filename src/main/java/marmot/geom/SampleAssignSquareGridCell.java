package marmot.geom;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import common.SampleUtils;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Size2d;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAssignSquareGridCell {
	private static final String INPUT = "POI/주유소_가격";
	private static final String SIDO = "구역/시도";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Envelope border = getBorder(marmot).getEnvelopeInternal();
		Size2d cellSize = new Size2d(border.getWidth() / 100, border.getHeight() / 100);
		
		Plan plan = Plan.builder("assign_fishnet_gridcell")
						.load(INPUT)
						.assignGridCell("the_geom", new SquareGrid(border, cellSize), false)
						.defineColumn("count:int", "1")
						.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_geom,cell_pos")
												.workerCount(11),
											AggregateFunction.SUM("count").as("count"))
						.expand("x:int,y:int", "x = cell_pos.x; y = cell_pos.y")
						.project("cell_geom as the_geom,x,y,count")
						.store(RESULT, FORCE)
						.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
	
	private static Geometry getBorder(MarmotRuntime marmot) {
		Plan plan = Plan.builder("get seould")
							.load(SIDO)
							.filter("ctprvn_cd == 11")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get();
	}
}
