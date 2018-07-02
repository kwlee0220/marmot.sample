package carloc;

import static marmot.optor.AggregateFunction.COUNT;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
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
	private static final String INPUT = Globals.TAXI_LOG_MAP;
	private static final String RESULT = "tmp/result";
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
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder("match_and_rank_roads")
					.load(INPUT)
					.filter("status == 0")
					.expand("hour:int", "hour=ts.substring(8,10)")
					.groupBy("hour,link_id,sub_link_no")
						.tagWith("link_geom")
						.aggregate(COUNT())
					.project("link_geom as the_geom,*-{link_geom}")
					.filter("count >= 50")
					.groupBy("hour")
						.tagWith("the_geom")
						.orderBy("count:D")
						.list()
					.store(RESULT)
					.build();

		DataSet result = marmot.createDataSet(RESULT, info, plan, true);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
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
								Charset.forName("euc-kr"), -1, false, false);
	}
}
