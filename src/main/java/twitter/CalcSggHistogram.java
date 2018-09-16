package twitter;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
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
public class CalcSggHistogram {
	private static final String LEFT_DATASET = "로그/social/twitter";
	private static final String RIGHT_DATASET = "구역/시군구";
	private static final String OUTPUT_DATASET = "/tmp/result";
	
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
		
		Plan plan = marmot.planBuilder("calc_emd_histogram")
								.loadSpatialIndexJoin(LEFT_DATASET, RIGHT_DATASET,
											SpatialRelation.INTERSECTS,
											"left.{id},right.{the_geom,SIG_CD,SIG_KOR_NM}")
								.groupBy("SIG_CD")
									.tagWith("the_geom,SIG_KOR_NM")
									.count()
								.store(OUTPUT_DATASET)
								.build();
		
		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		DataSet result = marmot.createDataSet(OUTPUT_DATASET, gcInfo, plan, DataSetOption.FORCE);
		watch.stop();

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
