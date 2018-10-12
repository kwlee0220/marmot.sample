package anyang.minwon;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S06_CountMinWonPerTeamGrid {
	private static final String MINWON = "기타/안양대/도봉구/민원";
	private static final String GRID = "기타/안양대/도봉구/GRID_100";
	private static final String OUTPUT = "분석결과/안양대/도봉구/격자별_팀별_민원수";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan;
		plan = marmot.planBuilder("격자별_팀별_민원수")
					.load(MINWON)
					.filter("team_name != null")
					.spatialJoin("the_geom", GRID,
									"team_name,param.{the_geom,spo_no_cd}")
					.groupBy("team_name,spo_no_cd")
						.tagWith("the_geom")
						.count()
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(MINWON).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
