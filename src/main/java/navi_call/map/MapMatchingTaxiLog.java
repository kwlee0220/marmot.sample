package navi_call.map;

import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;
import navi_call.Globals;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapMatchingTaxiLog {
	public static final void main(String... args) throws Exception {
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(Globals.TAXI_LOG);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		String geomCol = gcInfo.name();
		
//		String script = String.format("%s = ST_ClosestPointOnLine(%s, line)", geomCol, geomCol);
		
		Plan plan;
		plan = Plan.builder("택시로그_맵_매핑_org_road")
					.load(Globals.TAXI_LOG)
					.knnJoin(geomCol, Globals.ROADS, 1, 10, "*,param.{link_id,road_name}")
					.store(Globals.TAXI_LOG_MAP, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(Globals.TAXI_LOG_MAP);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
