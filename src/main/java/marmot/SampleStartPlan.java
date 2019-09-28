package marmot;

import static marmot.StoreDataSetOptions.FORCE;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.exec.MarmotExecution;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleStartPlan {
	private static final String INPUT = "주소/건물POI";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("sample_assign_uid")
							.load(INPUT)
							.filter("(long)출입구일련번호 % 119999 == 3")
							.assignUid("guid")
							.project("the_geom,guid,출입구일련번호")
							.store(RESULT, FORCE(gcInfo))
							.build();
		
		MarmotExecution exec = marmot.start(plan);
		System.out.println(exec.getState());
		exec.waitForFinished(1, TimeUnit.SECONDS);
		System.out.println(exec.getState());
		
		if ( !exec.waitForFinished(5, TimeUnit.SECONDS) ) {
			exec.cancel();
		}
		
//		while ( !exec.waitForFinished(5, TimeUnit.SECONDS) ) {
//			System.out.println(exec.getState());
//		}
		System.out.println(exec.getState());
		
//		DataSet result = marmot.getDataSet(RESULT);
//		SampleUtils.printPrefix(result, 5);
	}
}
