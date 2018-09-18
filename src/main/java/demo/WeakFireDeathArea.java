package demo;

import static marmot.DataSetOption.FORCE;

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
public class WeakFireDeathArea {
	private static final String LAYER_HOSPITAL = "시연/hospitals";
	private static final String LAYER_SEOUL = "시연/demo_seoul";
	private static final String LAYER_FIRE = "시연/fire_death";
	private static final String SRID = "EPSG:5186";
	
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
		
		Plan plan;
		DataSet result;
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", SRID);
		
		// 서울시 종합병원 위치에서 3km 버퍼 연산을 취해 clustered layer를 생성한다.
		Plan plan0 = marmot.planBuilder("서울시 종합병원 위치에서 3km 버퍼 연산")
								.load(LAYER_HOSPITAL)
								.buffer("the_geom", 3000)
								.store("tmp/종합병원_위치_추천/병원_3km버퍼")
								.build();
		marmot.createDataSet("tmp/종합병원_위치_추천/병원_3km버퍼",  gcInfo, plan0, true);
		System.out.println("완료: 서울시 종합병원 위치에서 3km 버퍼 영역 생성");

		// 서울시 지도에서 종합병원 3km 이내 영역과 겹치지 않은 영역을 계산한다.
		Plan plan1 = marmot.planBuilder("종합병원 3km 밖의 서울시 영역 계산")
								.load(LAYER_SEOUL)
								.differenceJoin("the_geom", "tmp/종합병원_위치_추천/병원_3km버퍼")
								.store("tmp/종합병원_위치_추천/병원_원거리_영역")
								.build();
		marmot.createDataSet("tmp/종합병원_위치_추천/병원_원거리_영역", gcInfo, plan1, true);
		System.out.println("완료: 종합병원 3km 밖의 서울시 영역 계산");
		
		// 화재패해 영역에서 서울 지역 부분만 선택한다.
		plan = marmot.planBuilder("seoul_fire_death")
					.load(LAYER_FIRE)
					.filter("sd_nm == '서울특별시'")
					.store("tmp/종합병원_위치_추천/서울_화재")
					.build();
		result = marmot.createDataSet("tmp/종합병원_위치_추천/서울_화재", gcInfo, plan, FORCE);

		// 화재피해 영역 중에서 서울 읍면동과 겹치는 부분을 clip 하고, 각 피해 영역의 중심점을 구한다.
		Plan plan2 = marmot.planBuilder("clip_bad_area")
								.load(LAYER_SEOUL)
								.clipJoin("the_geom", "tmp/종합병원_위치_추천/서울_화재")
								.centroid("the_geom")
								.clipJoin("the_geom", "tmp/종합병원_위치_추천/병원_원거리_영역")
								.store("tmp/종합병원_위치_추천/분석결과")
								.build();
		result = marmot.createDataSet("tmp/종합병원_위치_추천/분석결과", gcInfo, plan2, true);
		System.out.println("완료: 종합병원 3km 밖의 서울시 영역과 화재피해 영역과 교차부분 검색");
		
		marmot.deleteDataSet("tmp/종합병원_위치_추천/병원_3km버퍼");
		marmot.deleteDataSet("tmp/종합병원_위치_추천/병원_원거리_영역");
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
