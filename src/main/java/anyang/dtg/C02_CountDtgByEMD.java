package anyang.dtg;

import static marmot.StoreDataSetOptions.*;
import static marmot.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordScript;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.optor.JoinOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class C02_CountDtgByEMD {
	private static final String DTG = "교통/dtg";
	private static final String EMD_WGS84 = "분석결과/안양대/네트워크/읍면동_wgs84";
	private static final String EMD = "구역/읍면동";
	private static final String OUTPUT = "분석결과/안양대/네트워크/전국_읍면동별_통행량";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		RecordScript script = RecordScript.of("$pat = ST_DTPattern(\"yyyyMMddHHmmss\")",
										"ST_DTParseLE(운행일자 + 운행시분초.substring(0,6), $pat)");
		Plan aggrPlan = marmot.planBuilder("aggregate")
								.clusterChronicles("ts", "interval", "10m")
								.aggregate(AggregateFunction.COUNT())
								.build();

		Plan plan;
		plan = marmot.planBuilder("전국_읍면동별_통행량")
					.load(DTG)
					.filter("운행속도 > 0")
					.defineColumn("ts:datetime", script)
					.spatialJoin("the_geom", EMD_WGS84, "param.{emd_cd},차량번호 as car_no,ts")
					.expand("emd_cd:int")
					.runPlanByGroup(Group.ofKeys("emd_cd,car_no")
										.workerCount(57), aggrPlan)
					.aggregateByGroup(Group.ofKeys("emd_cd"),
									AggregateFunction.SUM("count").as("count"))
					.expand("emd_cd:string")
					.hashJoin("emd_cd", EMD, "emd_cd", "param.{the_geom,emd_kor_nm},count",
							JoinOptions.INNER_JOIN(1))
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(EMD).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, FORCE(gcInfo));
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
