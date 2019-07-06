package demo.policy;


import static marmot.StoreDataSetOptions.*;
import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step03 {
	static final String INPUT = "구역/연속지적도_2017";
	private static final String PARAM = Step02.RESULT;
	static final String RESULT = "tmp/10min/step03";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("노인복지시설필요지역추출")
						.load(INPUT)
						.spatialSemiJoin(gcInfo.name(), PARAM, SpatialJoinOptions.NEGATED) // (3) 교차반전
						.store(RESULT)
						.build();
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE(gcInfo));
		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
