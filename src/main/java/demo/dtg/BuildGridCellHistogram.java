package demo.dtg;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildGridCellHistogram {
	private static final String TAGGED = "tmp/dtg/taggeds";
	private static final String RESULT = "tmp/dtg/histogram_grid";
	
	private static final int WORKER_COUNT = 5;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet output;

		Plan plan;
		plan = marmot.planBuilder("build_histogram_grid")
					.load(TAGGED)
					.aggregateByGroup(Group.ofKeys("cell_id").tags("grid")
											.workerCount(WORKER_COUNT),
										COUNT())
					.project("grid as the_geom,count")
					.store(RESULT)
					.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		output = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		
		watch.stop();
		System.out.printf("count=%d, total elapsed time=%s%n",
							output.getRecordCount(), watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(output, 5);
	}
}
