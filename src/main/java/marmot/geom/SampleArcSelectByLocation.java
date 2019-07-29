package marmot.geom;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.plan.SpatialJoinOptions.WITHIN_DISTANCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcSelectByLocation {
	private static final String INPUT1 = "안양대/공간연산/select/input1";
	private static final String INPUT2 = "안양대/공간연산/select/input2";
	private static final String RESULT = "tmp/result";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT1).getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("sample_arc_dissolve")
							.load(INPUT1)
							.spatialSemiJoin(gcInfo.name(), INPUT2, WITHIN_DISTANCE(50))
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE(gcInfo));
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
