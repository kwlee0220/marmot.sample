package demo.policy;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildTenMinutePolicyFull {
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
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		DataSet output;
		
		// '노인복지시설_경로당_추출_버퍼' 추출
		StopWatch watch2 = StopWatch.start();
		output = bufferElderlyCareFacilities(marmot);
		output.cluster();
		System.out.println("완료: '노인복지시설_경로당_추출_버퍼' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());
		
		// '인구밀도_2017_중심점추출_10000이상' 추출
		watch2 = StopWatch.start();
		output = findHighPopulationDensity(marmot);
		output.cluster();
		System.out.println("완료: '인구밀도_2017_중심점추출_10000이상' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());
		
		// 인구밀도_10000이상_행정동추출
		watch2 = StopWatch.start();
		output = findHighPopulationHDong(marmot);
		output.cluster();
		System.out.println("완료: '인구밀도_10000이상_행정동' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());

		watch2 = StopWatch.start();
		DataSet ds = marmot.getDataSet(CADASTRAL);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("경로당필요지역추출")
						.load(CADASTRAL)
						.project("the_geom,pnu")
						.spatialSemiJoin(info.name(), ELDERLY_CARE_BUFFER, INTERSECTS, true)	// (3) 교차반전
						.clipJoin(info.name(), HIGH_DENSITY_HDONG)			// (7) 클립분석
						.shard(1)
						.store(RESULT)
						.build();
		output = marmot.createDataSet(RESULT, info, plan, true);
		output.cluster();
		System.out.println("완료: '경로당필요지역' 추출, elapsed="
							+ watch2.stopAndGetElpasedTimeString());
		
		watch.stop();
		System.out.printf("count=%d, total elapsed time=%s%n",
							output.getRecordCount(), watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(output, 5);
	}
	
	private static DataSet bufferElderlyCareFacilities(PBMarmotClient marmot) {
		DataSet ds = marmot.getDataSet(ELDERLY_CARE);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("노인복지시설_경로당_추출_버퍼")
					.load(ELDERLY_CARE)
					.filter("induty_nm == '경로당'")			// (1) 영역분석
					.project("the_geom")
					.buffer("the_geom", "the_geom", 400)	// (2) 버퍼추정
					.store(ELDERLY_CARE_BUFFER)
					.build();
		return marmot.createDataSet(ELDERLY_CARE_BUFFER, info, plan, true);
	}
	
	private static DataSet findHighPopulationDensity(PBMarmotClient marmot) {
		DataSet ds = marmot.getDataSet(POP_DENSITY);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("인구밀도_2017_중심점추출_10000이상")
					.load(POP_DENSITY)
					.centroid("the_geom", "the_geom")						// (4) 중심점 추출
					.filter("value >= 10000")								// (5) 영역분석
					.project("the_geom")
					.store(HIGH_DENSITY_CENTER)
					.build();
		return marmot.createDataSet(HIGH_DENSITY_CENTER, info, plan, true);
	}
	
	private static DataSet findHighPopulationHDong(PBMarmotClient marmot) {
		DataSet ds = marmot.getDataSet(HDONG);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("인구밀도_10000이상_행정동추출")
					.load(HDONG)
					.project("the_geom")
					.spatialSemiJoin("the_geom", HIGH_DENSITY_CENTER, INTERSECTS, false)	// (6) 교차분석
					.project("the_geom")
					.store(HIGH_DENSITY_HDONG)
					.build();
		return marmot.createDataSet(HIGH_DENSITY_HDONG, info, plan, true);
	}
}