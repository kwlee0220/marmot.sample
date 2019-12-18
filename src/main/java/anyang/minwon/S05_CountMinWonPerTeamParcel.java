package anyang.minwon;

import static marmot.optor.JoinOptions.RIGHT_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S05_CountMinWonPerTeamParcel {
	private static final String MINWON = "기타/안양대/도봉구/민원";
	private static final String PARCEL = "기타/안양대/도봉구/필지";
	private static final String OUTPUT = "분석결과/안양대/도봉구/팀별_필지별_민원수";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = marmot.getDataSet(PARCEL).getGeometryColumnInfo();
		
		Plan plan;		
		plan = Plan.builder("담당팀_필지별_민원수 합계")
					.load(MINWON)
					.filter("team_name != null")
					.hashJoin("all_parcel_layer_id", PARCEL, "id", 
							"param.{the_geom,id},team_name", RIGHT_OUTER_JOIN)
					.aggregateByGroup(Group.ofKeys("team_name,id").tags("the_geom"),
										AggregateFunction.COUNT())
					.store(OUTPUT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.close();
	}
}
