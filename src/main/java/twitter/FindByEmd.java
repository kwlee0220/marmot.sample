package twitter;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindByEmd {
	private static final String TWEETS = "로그/social/twitter";
	private static final String RESULT = "/tmp/result";
	
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
		
		// 강남구의 행정 영역 정보를 획득한다.
		Geometry border = getBorder(marmot);

		// 생성될 결과 레이어의 좌표체계를 위해 tweet 레이어의 것도 동일한 것을
		// 사용하기 위해 tweet 레이어의 정보를 서버에서 얻는다.
		DataSet info = marmot.getDataSet(TWEETS);

		// 프로그램 수행 이전에 기존 OUTPUT_LAYER을 제거시킨다.
		marmot.deleteDataSet(RESULT);
		
		Plan plan = marmot.planBuilder("find_emd")
							// tweet 레이어를 읽어, 서초동 행정 영역과 겹치는 트위 레코드를 검색한다.
							.query(TWEETS, SpatialRelation.INTERSECTS, border)
							.project("the_geom,id")
							// 검색된 레코드를 'OUTPUT_LAYER' 레이어에 저장시킨다.
							.store(RESULT)
							.build();

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom",
															info.getGeometryColumnInfo().srid());
		DataSet result = marmot.createDataSet(RESULT, gcInfo, plan, DataSetOption.FORCE);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}

	private static final String EMD = "구역/읍면동";
	private static Geometry getBorder(MarmotRuntime marmot) throws Exception {
		// '읍면동 행정구역' 레이어에서 강남구 행정 영역 정보를 검색하는 프로그램을 구성한다.
		//
		Plan plan = marmot.planBuilder("find_emd")
								// 읍면동 행정구역 레이어를 읽는다.
								.load(EMD)
								// 강남구 레코드를 검색한다.
								.filter("emd_kor_nm=='서초동'")
								// 강남구 행정 영역 컬럼만 뽑는다.
								.project("the_geom")
								.build();
		// 프로그램 수행으로 생성된 임시 레이어를 읽어 강남구 영역을 읽는다.
		return marmot.executeLocally(plan)
						.stream()
						.findAny().get()
						.getGeometry(0);
	}
}
