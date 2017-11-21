package carloc;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapMatchingTaxiLog {
	private static final String EMD = "구역/읍면동";
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
//	private static final String TAXI_LOG_DONG = "tmp/택시로그_동";
	private static final String TAXI_LOG_DONG = TAXI_LOG;
	private static final String ROADS = "교통/도로/링크";
	private static final String RESULT = "tmp/result";
	private static final double DISTANCE = 10;
	private static final String DONG = "방배동";
	
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
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		// 관악구내 로그만 추출
//		projectToDong(marmot);
		
		DataSet input = marmot.getDataSet(TAXI_LOG_DONG);
		String geomCol = input.getGeometryColumn();
		String srid = input.getSRID();
		
		Plan plan;
		plan = marmot.planBuilder("택시로그_맵_매핑")
					.load(TAXI_LOG_DONG)
					.mapMatchingJoin(geomCol, ROADS, geomCol, DISTANCE, "")
					.store(RESULT)
					.build();

		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		DataSet result = marmot.createDataSet(RESULT, schema, geomCol, srid, true);
		marmot.execute(plan);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedTimeString());
	}
	
	private static final void projectToDong(MarmotClient marmot) throws Exception {
		StopWatch watch = StopWatch.start();
		
		Geometry guBoundary = getDongBoundary(marmot, DONG);
		
		DataSet input = marmot.getDataSet(TAXI_LOG);
		String geomCol = input.getGeometryColumn();
		String srid = input.getSRID();
		
		Plan plan;
		plan = marmot.planBuilder("동내_로그_추출")
					.load(TAXI_LOG)
					.intersects(geomCol, guBoundary)
					.store(TAXI_LOG_DONG)
					.build();

		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		DataSet result = marmot.createDataSet(TAXI_LOG_DONG, schema, geomCol, srid, true);
		marmot.execute(plan);
		watch.stop();

		System.out.printf("count=%d elapsed=%s%n", result.getRecordCount(),
													watch.getElapsedTimeString());
	}
	
	private static Geometry getDongBoundary(MarmotClient marmot, String dongName)
		throws Exception {
		String predicate = String.format("emd_kor_nm == '%s'", dongName);
		Plan plan = marmot.planBuilder("filter")
							.load(EMD)
							.filter(predicate)
							.project("the_geom")
							.build();
		return marmot.executeLocally(plan)
						.stream()
						.map(rec -> rec.getGeometry(0))
						.findAny()
						.orElse(null);
	}
}
