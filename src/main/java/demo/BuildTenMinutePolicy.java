package demo;

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
public class BuildTenMinutePolicy {
	private static final String ELDERLY_CARE = "eldely_care_facilites";
	private static final String CADASTRAL = "구역/연속지적도_2017";
	private static final String POP_DENSITY = "population_density";
	private static final String EMD = "구역/읍면동";
	
	private static final String HIGH_DENSITY_CENTERS = "tmp/10min/high_density_centers";
	private static final String ELDERLY_CARE_BUFFER = "tmp/10min/eldely_care_facilites_bufferred";
	private static final String RESULT = "tmp/result";
	
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
		
		// '노인복지시설_경로당_추출_버퍼' 추출
		DataSet buffered = bufferElderlyCareFacilities(marmot);
		buffered.cluster();
		
		// '인구밀도_2017_중심점추출_10000이상' 추출
		DataSet highPop = findHighPopulationDensity(marmot);
		buffered.cluster();
		
		DataSet ds = marmot.getDataSet(CADASTRAL);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder("경로당필요지역추출_20180412")
					.load(CADASTRAL)
					.differenceJoin(info.name(), buffered.getId())	// (3) 교차반전
					.clipJoin(info.name(), highPop.getId())			// (7) 클립분석
					.store(RESULT)
					.build();
		DataSet result = marmot.createDataSet(RESULT, info, plan, true);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
	
	private static DataSet bufferElderlyCareFacilities(PBMarmotClient marmot) {
		DataSet ds = marmot.getDataSet(ELDERLY_CARE);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();

		Plan plan;
		plan = marmot.planBuilder("노인복지시설_경로당_추출_버퍼")
					.load(ELDERLY_CARE)
					.filter("induty_nm == '경로당'")			// (1) 영역분석
					.buffer(info.name(), info.name(), 400)	// (2) 버퍼추정
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
					.centroid(info.name(), info.name())						// (4) 중심점 추출
					.filter("value >= 10000")								// (5) 영역분석
					.spatialJoin(info.name(), EMD, INTERSECTS, "param.*")	// (6) 교차분석
					.store(HIGH_DENSITY_CENTERS)
					.build();
		return marmot.createDataSet(HIGH_DENSITY_CENTERS, info, plan, true);
	}
}
