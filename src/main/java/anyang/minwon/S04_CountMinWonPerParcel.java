package anyang.minwon;

import static marmot.DataSetOption.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotClient;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S04_CountMinWonPerParcel {
	private static final String MINWON = "기타/안양대/도봉구/민원";
	private static final String PARCEL = "기타/안양대/도봉구/필지";
	private static final String OUTPUT = "분석결과/안양대/민원/필지별_민원수";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();

		Plan plan;
		plan = marmot.planBuilder("연별 가스 사용량 합계")
					.loadEquiJoin(MINWON, "all_parcel_layer_id", PARCEL, "id",
									"right.{the_geom,id}, left.*-{the_geom,all_parcel_layer_id}",
									JoinOptions.INNER_JOIN())
					.groupBy("id")
						.tagWith("the_geom")
						.count()
					.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
