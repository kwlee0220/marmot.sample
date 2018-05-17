package carloc;

import static marmot.optor.AggregateFunction.COUNT;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindBestRoadsForPickup {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String ROADS = "교통/도로/링크";
	private static final String RESULT = "tmp/result";
	private static final String SRID = "EPSG:5186";
	private static final double DISTANCE = 5;
	
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
		plan = marmot.planBuilder("match_and_rank_roads")
					.load(TAXI_LOG)
					.filter("status == 0")
					.expand("hour:int", "hour=ts.substring(8,10)")
					.knnJoin("the_geom", ROADS, 10, 1,
							"hour,car_no,param.{LINK_ID,the_geom,ROAD_NAME,ROADNAME_A}")
					.groupBy("hour,LINK_ID")
						.taggedKeyColumns("the_geom,ROAD_NAME,ROADNAME_A")
						.aggregate(COUNT())
					.filter("count >= 50")
					.groupBy("hour")
						.orderBy("count:D")
						.list()
					.storeMarmotFile(RESULT)
					.build();

		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 100);
	}
	
	private static void exportResult(PBMarmotClient marmot, String resultLayerName,
									String baseDirPath) throws IOException {
		export(marmot, resultLayerName, 8, baseDirPath);
		export(marmot, resultLayerName, 22, baseDirPath);
	}
	
	private static void export(PBMarmotClient marmot, String resultLayerName, int hour,
								String baseName) throws IOException {
		Plan plan = marmot.planBuilder("export")
								.load(resultLayerName)
								.filter("hour == " + hour)
								.build();
		RecordSet rset = marmot.executeLocally(plan);

		String file = String.format("/home/kwlee/tmp/%s_%02d.shp", baseName, hour);
		marmot.writeToShapefile(rset, new File(file), "best_roads", SRID,
								Charset.forName("euc-kr"), false, false);
	}
}
