package anyang.dtg;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.plan.RecordScript;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class C02_CountDtgByEMD {
	private static final String DTG = "교통/dtg_s";
	private static final String EMD = "분석결과/안양대/네트워크/읍면동_wgs84";
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
		GeometryColumnInfo dtgInfo = marmot.getDataSet(DTG).getGeometryColumnInfo();
		GeometryColumnInfo emdInfo = marmot.getDataSet(EMD).getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("전국_읍면동별_통행량")
					.load(DTG)
					.filter("운행속도 > 0")
					.spatialJoin("the_geom", EMD,
								"param.{the_geom,emd_cd,emd_kor_nm},"
								+ "차량번호 as car_no,운행일자,운행시분초")
					.expand1("ts:datetime", script)
					.expand("emd_cd:int")
					.project("*-{운행일자,운행시분초}")
					.groupBy("emd_cd,car_no")
						.tagWith("the_geom,emd_kor_nm")
						.run(aggrPlan)
					.groupBy("emd_cd")
						.tagWith("the_geom,emd_kor_nm")
						.aggregate(AggregateFunction.SUM("count").as("count"))
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(EMD).getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
