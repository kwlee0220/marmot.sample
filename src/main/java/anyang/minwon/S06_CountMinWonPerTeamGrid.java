package anyang.minwon;

import static marmot.optor.StoreDataSetOptions.FORCE;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S06_CountMinWonPerTeamGrid {
	private static final String MINWON = "기타/안양대/도봉구/민원";
	private static final String GRID = "기타/안양대/도봉구/GRID_100";
	private static final String OUTPUT = "분석결과/안양대/도봉구/격자별_팀별_민원수";
	
	public static final void main(String... args) throws Exception {
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = marmot.getDataSet(MINWON).getGeometryColumnInfo();

		Plan plan;
		plan = Plan.builder("격자별_팀별_민원수")
					.load(MINWON)
					.filter("team_name != null")
					.spatialJoin("the_geom", GRID,
									"team_name,param.{the_geom,spo_no_cd}")
					.aggregateByGroup(Group.ofKeys("team_name,spo_no_cd").tags("the_geom"),
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
