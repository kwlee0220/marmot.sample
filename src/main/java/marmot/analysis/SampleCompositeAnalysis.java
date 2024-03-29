package marmot.analysis;

import static marmot.optor.StoreDataSetOptions.FORCE;

import marmot.Plan;
import marmot.analysis.system.SystemAnalysis;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.exec.CompositeAnalysis;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotExecution;
import marmot.exec.PlanAnalysis;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleCompositeAnalysis {
	private static final String INPUT = "주소/건물POI";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		DataSet ds;
		GeometryColumnInfo gcInfo;
		
		marmot.deleteAnalysis("10min", true);
		
		gcInfo = marmot.getDataSet("POI/노인복지시설").getGeometryColumnInfo();
		plan = Plan.builder("노인복지시설_경로당_버퍼")
						.load("POI/노인복지시설")
						.filter("induty_nm == '경로당'")
						.project("the_geom")
						.buffer("the_geom", 400)
						.store("/tmp/10min/노인복지시설_경로당_버퍼", FORCE(gcInfo))
						.build();
		marmot.addAnalysis(new PlanAnalysis("10min/노인복지시설_경로당_버퍼", plan), true);
		marmot.addAnalysis(SystemAnalysis.clusterDataSet("10min/노인복지시설_경로당_버퍼_색인",
															"/tmp/10min/노인복지시설_경로당_버퍼"), true);

		gcInfo = marmot.getDataSet("주민/인구밀도_2000").getGeometryColumnInfo();
		plan = Plan.builder("10000이상_인구밀도_중심점_추출")
						.load("주민/인구밀도_2000")
						.centroid("the_geom")
						.filter("value >= 10000")
						.project("the_geom")
						.store("/tmp/10min/10000이상_인구밀도_중심점", FORCE(gcInfo))
						.build();
		marmot.addAnalysis(new PlanAnalysis("10min/10000이상_인구밀도_중심점_추출", plan), true);
		marmot.addAnalysis(SystemAnalysis.clusterDataSet("10min/10000이상_인구밀도_중심점_색인",
															"/tmp/10min/10000이상_인구밀도_중심점"), true);
		
		gcInfo = marmot.getDataSet("구역/행정동코드").getGeometryColumnInfo();
		plan = Plan.builder("10000이상_인구밀도_행정동_추출")
						.load("구역/행정동코드")
						.project("the_geom")
						.spatialSemiJoin("the_geom", "/tmp/10min/10000이상_인구밀도_중심점")
						.store("/tmp/10min/10000이상_인구밀도_행정동", FORCE(gcInfo))
						.build();
		marmot.addAnalysis(new PlanAnalysis("10min/10000이상_인구밀도_행정동_추출", plan), true);
		marmot.addAnalysis(SystemAnalysis.clusterDataSet("10min/10000이상_인구밀도_행정동_색인",
																"/tmp/10min/10000이상_인구밀도_행정동"), true);
		
		gcInfo = marmot.getDataSet("구역/연속지적도_2017").getGeometryColumnInfo();
		plan = Plan.builder("경로당필요지역_추출")
						.load("구역/연속지적도_2017")
						.project("the_geom,pnu")
						.spatialSemiJoin("the_geom", "/tmp/10min/노인복지시설_경로당_버퍼",
										SpatialJoinOptions.NEGATED)
						.arcClip("the_geom", "/tmp/10min/10000이상_인구밀도_행정동")
						.shard(1)
						.store("/분석결과/10min/경로당필요지역", FORCE(gcInfo))
						.build();
		marmot.addAnalysis(new PlanAnalysis("10min/경로당필요지역_추출", plan), true);
		marmot.addAnalysis(SystemAnalysis.clusterDataSet("10min/경로당필요지역_색인",
																"/분석결과/10min/경로당필요지역"), true);
		
		marmot.addAnalysis(SystemAnalysis.deleteDataSet("10min/경로당_버퍼_파일 제거",
														"/tmp/10min/노인복지시설_경로당_버퍼"), true);
		marmot.addAnalysis(SystemAnalysis.deleteDataSet("10min/인구밀도_중심점_제거",
														"/tmp/10min/10000이상_인구밀도_중심점"), true);
		marmot.addAnalysis(SystemAnalysis.deleteDataSet("10min/인구밀도_행정동_제거",
														"/tmp/10min/10000이상_인구밀도_행정동"), true);
		marmot.addAnalysis(new CompositeAnalysis("10min",
														"10min/노인복지시설_경로당_버퍼",
														"10min/노인복지시설_경로당_버퍼_색인",
														"10min/10000이상_인구밀도_중심점_추출",
														"10min/10000이상_인구밀도_중심점_색인",
														"10min/10000이상_인구밀도_행정동_추출",
														"10min/10000이상_인구밀도_행정동_색인",
														"10min/경로당필요지역_추출",
														"10min/경로당필요지역_색인",
														"10min/임시파일 제거"), true);
		
		MarmotAnalysis anal = marmot.getAnalysis("10min");
		System.out.println(anal);
		CompositeAnalysis composite = (CompositeAnalysis)anal;
		
		MarmotExecution exec = marmot.startAnalysis(composite);
		while ( exec.isRunning() ) {
			System.out.printf("state=%s, index=%d%n",
							exec.getState(),  exec.getCurrentExecutionIndex());
			Thread.sleep(5000);
		}
		System.out.printf("state=%s, index=%d%n",
						exec.getState(),  exec.getCurrentExecutionIndex());
	}
}
