package navi_call.map;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import navi_call.Globals;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S1_MapMatchingTaxiLog {
	private static final String INPUT = Globals.TAXI_LOG;
	private static final String PARAM = Globals.ROADS;
	private static final String RESULT = Globals.TAXI_LOG_MAP;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		String geomCol = gcInfo.name();
		
		String script = String.format("%s = ST_ClosestPointOnLine(%s, line)", geomCol, geomCol);
		
		Plan plan;
		plan = marmot.planBuilder("택시로그_맵_매핑_org_road")
					.load(INPUT)
					.knnJoin(geomCol, PARAM, 1, Globals.DISTANCE,
							"*,param.{the_geom as link_geom, link_id}")
//					.update(script)
					.store(RESULT)
					.build();
		DataSet result = marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
