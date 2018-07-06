package demo.dtg;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BufferRoadLinks {
	private static final String ROAD = "교통/도로/링크";
	private static final String RESULT = "tmp/dtg/road_buffered";
	
	private static final double BUFFER_DIST = 25d;
	private static final long GEOM_IDX_BLK_SIZE = UnitUtils.parseByteSize("32mb");
	private static final int IDX_WORKER_COUNT = 1;
	
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
		
		DataSet ds = marmot.getDataSet(ROAD);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("build_dtg_histogram")
							.load(ROAD)
							.buffer("the_geom", "the_geom", BUFFER_DIST)
							.store(RESULT)
							.build();
		DataSet result = marmot.createDataSet(RESULT, info, plan, true);
		
		System.out.printf("done[buffer]: count=%d, elapsed time=%s%n",
							result.getRecordCount(), watch.getElapsedMillisString());
		
		StopWatch idxWatch = StopWatch.start();
		System.out.println("start spatial indexing...");
		result.cluster(ClusterDataSetOptions.create()
											.blockSize(GEOM_IDX_BLK_SIZE)
											.workerCount(IDX_WORKER_COUNT));
		System.out.printf("done[indexing]:elapsed time=%s%n",
							idxWatch.getElapsedMillisString());
		
		watch.stop();
		System.out.printf("done: total_elapsed time=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
