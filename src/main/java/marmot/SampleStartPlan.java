package marmot;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.concurrent.TimeUnit;

import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.exec.MarmotExecution;
import marmot.exec.PlanAnalysis;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleStartPlan {
	private static final String INPUT = "주소/건물POI";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		Plan plan = Plan.builder("sample_assign_uid")
							.load(INPUT)
							.filter("(long)출입구일련번호 % 119999 == 3")
							.assignUid("guid")
							.project("the_geom,guid,출입구일련번호")
							.store(RESULT, FORCE(gcInfo))
							.build();
		
		MarmotExecution exec = marmot.startAnalysis(new PlanAnalysis("noname", plan));
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
