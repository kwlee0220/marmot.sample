package anyang.minwon;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S03_CountParkMinWonPerParcel {
	private static final String PARK_MINWON = "기타/안양대/도봉구/공원_민원";
	private static final String PARCEL = "기타/안양대/도봉구/필지";
	private static final String OUTPUT = "분석결과/안양대/도봉구/필지별_공원민원수";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan;		
		plan = marmot.planBuilder("필지별 공원관련 민원수 합계")
					.load(PARK_MINWON)
					.join("all_parcel_layer_id", PARCEL, "id", 
							"param.{the_geom,id},team_name",
							JoinOptions.RIGHT_OUTER_JOIN())
					.groupBy("id")
						.tagWith("the_geom")
						.count()
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(PARCEL).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
