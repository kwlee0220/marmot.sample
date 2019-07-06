package demo.policy;

import static marmot.ExecutePlanOptions.DISABLE_LOCAL_EXEC;
import static marmot.StoreDataSetOptions.FORCE;

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
public class Step02 {
	private static final String INPUT = Step01.RESULT;
	static final String RESULT = "tmp/10min/step02";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("노인복지시설_경로당_추출_버퍼")
						.load(INPUT)
						.buffer(gcInfo.name(), 400)	// (2) 버퍼추정
						.build();
		DataSet result = marmot.createDataSet(RESULT, plan, DISABLE_LOCAL_EXEC, FORCE(gcInfo));
		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
