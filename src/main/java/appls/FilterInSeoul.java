package appls;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.MarmotRuntime;
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
public class FilterInSeoul {
	private static final String SID = "구역/시도";
	private static final String LAND_USAGE = "토지/토지이용계획_누적";
	private static final String CADASTRAL = "구역/연속지적도";
	private static final String CADASTRAL_SEOUL = "tmp/seoul";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
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
		
		DataSet ds = marmot.getDataSet(LAND_USAGE);
//		String geomCol = ds.getGeometryColumn();
//		String srid = ds.getSRID();
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		Geometry seoul = getSeoulBoundary(marmot);
		getSeoulCadastral(marmot, seoul, CADASTRAL_SEOUL);
		
		plan = marmot.planBuilder("tag_geom")
					.load(LAND_USAGE)
					.filter("법정동코드.startsWith('11')")
					.join("고유번호", CADASTRAL_SEOUL, "pnu", "*,param.the_geom", null)
					.project("the_geom, 고유번호 as pnu, 용도지역지구코드 as code, 용도지역지구명 as name")
					.store(RESULT)
					.build();
		result = marmot.createDataSet(RESULT, plan, true);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
	
	private static Geometry getSeoulBoundary(MarmotRuntime marmot) {
		Plan plan;
		
		DataSet sid = marmot.getDataSet(SID);
		plan = marmot.planBuilder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.build();
		return marmot.executeLocally(plan).toList().get(0)
										.getGeometry(sid.getGeometryColumnInfo().name());
	}
	
	private static void getSeoulCadastral(MarmotRuntime marmot, Geometry seoul, String output) {
		Plan plan;
		
		DataSet taxi = marmot.getDataSet(CADASTRAL);
		
		plan = marmot.planBuilder("grid_taxi_logs")
					// 택시 로그를  읽는다.
					.load(CADASTRAL, INTERSECTS, seoul)
					// 승하차 로그만 선택한다.
					.filter("pnu.startsWith('11')")
					.store(output)
					.build();
		marmot.createDataSet(output, taxi.getGeometryColumnInfo(), plan, true);
	}
}
