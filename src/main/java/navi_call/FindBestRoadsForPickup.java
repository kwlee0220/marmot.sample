package navi_call;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindBestRoadsForPickup {
	private static final String RESULT = "분석결과/시간별_택시_픽업_최적지";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet ds = marmot.getDataSet(Globals.TAXI_LOG_MAP);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder("시간별_택시_픽업_최적지")
					.load(Globals.TAXI_LOG_MAP)
					.filter("status == 0")
					.defineColumn("hour:int", "ts.substring(8,10)")
					.aggregateByGroup(Group.ofKeys("hour,link_id")
											.withTags("the_geom,road_name"), COUNT())
					.filter("count >= 50")
					.storeByGroup(Group.ofKeys("hour"), RESULT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
	}
}
