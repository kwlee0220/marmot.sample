package marmot;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotAnalysis.Type;
import marmot.exec.MarmotExecution;
import marmot.exec.PlanAnalysis;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleRegisterPlanAnalytics {
	private static final String INPUT = "주소/건물POI";
	private static final String RESULT = "tmp/result";
	private static final String ANA_ID = "/tmp/test";
	
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
		
		marmot.deleteAnalysis(ANA_ID, true);
		
		PlanAnalysis analytics = new PlanAnalysis(ANA_ID, plan);
		Utilities.checkState(analytics.getId().equals(ANA_ID));
		Utilities.checkState(analytics.getType() == Type.PLAN);
		marmot.addAnalysis(analytics, true);
		
		MarmotAnalysis analytics2 = marmot.getAnalysis(ANA_ID);
		Utilities.checkState(analytics2.getId().equals(ANA_ID));
		Utilities.checkState(analytics2.getType() == Type.PLAN);
		
		MarmotExecution exec = marmot.startAnalysis(analytics2);
		System.out.println(exec.getState());
		exec.waitForFinished(1, TimeUnit.SECONDS);
		System.out.println(exec.getState());
		
		if ( !exec.waitForFinished(5, TimeUnit.SECONDS) ) {
			exec.cancel();
		}
	}
}
