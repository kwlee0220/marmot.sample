package demo.policy;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.ExecutePlanOptions;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step06 {
	private static final String INPUT = "구역/행정동코드";
	private static final String PARAM = Step05.RESULT;
	static final String RESULT = "tmp/10min/step06";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("인구밀도_10000이상_행정동추출")
						.load(INPUT)
						.spatialSemiJoin(info.name(), PARAM)	// (6) 교차분석
						.build();
		DataSet result = marmot.createDataSet(RESULT, plan, ExecutePlanOptions.create().disableLocalExecution(true), StoreDataSetOptions.create().geometryColumnInfo(info).force(true));
		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
