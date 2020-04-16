package demo.policy;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.LoadOptions;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildTenMinutePolicy {
	private static final String ELDERLY_CARE = "POI/노인복지시설";
	private static final String CADASTRAL = "구역/연속지적도_2017";
	private static final String POP_DENSITY = "주민/인구밀도_2000";
	private static final String HDONG = "구역/행정동코드";
	
	private static final String ELDERLY_CARE_BUFFER = "tmp/10min/eldely_care_facilites_bufferred";
	private static final String HIGH_DENSITY_CENTER = "tmp/10min/high_density_center";
	private static final String HIGH_DENSITY_HDONG = "tmp/10min/high_density_hdong";
	private static final String RESULT = "tmp/10min/elderly_care_candidates";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet output;
		
		// '노인복지시설_경로당_추출_버퍼' 추출
		StopWatch watch2 = StopWatch.start();
		bufferElderlyCareFacilities(marmot);
		System.out.println("완료: '노인복지시설_경로당_추출_버퍼' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());
		
		// '인구밀도_2017_중심점추출_10000이상' 추출
		watch2 = StopWatch.start();
		findHighPopulationDensity(marmot);
		System.out.println("완료: '인구밀도_2017_중심점추출_10000이상' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());
		
		// 인구밀도_10000이상_행정동추출
		watch2 = StopWatch.start();
		findHighPopulationHDong(marmot);
		System.out.println("완료: '인구밀도_10000이상_행정동' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());

		watch2 = StopWatch.start();
		DataSet ds = marmot.getDataSet(CADASTRAL);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan = Plan.builder("경로당필요지역추출")
						.load(CADASTRAL, LoadOptions.FIXED_MAPPERS)
						.spatialSemiJoin(gcInfo.name(), ELDERLY_CARE_BUFFER,	// (3) 교차반전
										SpatialJoinOptions.NEGATED)
						.arcClip(gcInfo.name(), HIGH_DENSITY_HDONG)			// (7) 클립분석
						.store(RESULT, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		watch.stop();
		System.out.println("완료: '경로당필요지역' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		output = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(output, 5);

		System.out.printf("count=%d, total elapsed time=%s%n",
							output.getRecordCount(), watch.getElapsedMillisString());
	}
	
	private static void bufferElderlyCareFacilities(PBMarmotClient marmot) {
		DataSet ds = marmot.getDataSet(ELDERLY_CARE);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan;
		plan = Plan.builder("노인복지시설_경로당_추출_버퍼")
					.load(ELDERLY_CARE)
					.filter("induty_nm == '경로당'")			// (1) 영역분석
					.buffer(gcInfo.name(), 400)	// (2) 버퍼추정
					.store(ELDERLY_CARE_BUFFER, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
	}
	
	private static void findHighPopulationDensity(PBMarmotClient marmot) {
		DataSet ds = marmot.getDataSet(POP_DENSITY);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan;
		plan = Plan.builder("인구밀도_2017_중심점추출_10000이상")
					.load(POP_DENSITY)
					.filter("value >= 10000")						// (5) 영역분석
					.centroid(gcInfo.name())						// (4) 중심점 추출
					.store(HIGH_DENSITY_CENTER, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
	}
	
	private static void findHighPopulationHDong(PBMarmotClient marmot) {
		DataSet ds = marmot.getDataSet(HDONG);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan;
		plan = Plan.builder("인구밀도_10000이상_행정동추출")
					.load(HDONG)
					.spatialSemiJoin(gcInfo.name(), HIGH_DENSITY_CENTER)	// (6) 교차분석
					.store(HIGH_DENSITY_HDONG, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
	}
}
