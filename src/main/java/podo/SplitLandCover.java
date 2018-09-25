package podo;

import java.util.concurrent.CompletableFuture;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.plan.LoadOption;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SplitLandCover {
	private static final String LAND_COVER_1987 = "토지/토지피복도/1987";
	private static final String LAND_COVER_2007 = "토지/토지피복도/2007";
	private static final String OUTPUT_1987_S = "토지/토지피복도/1987S";
	private static final String OUTPUT_2007_S = "토지/토지피복도/2007S";
	
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
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		StopWatch watch = StopWatch.start();
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> split(marmot, LAND_COVER_1987, OUTPUT_1987_S));
		Thread.sleep(120 * 1000);
		split(marmot, LAND_COVER_2007, OUTPUT_2007_S);
		future.join();
		
		System.out.println("완료: 토지피복도 분할");
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
	}
	
	private static void split(PBMarmotClient marmot, String inputDsId, String outputDsId) {
		DataSet ds = marmot.getDataSet(inputDsId);
		String geomCol = ds.getGeometryColumn();
		
		Plan plan = marmot.planBuilder(inputDsId + "_분할")
							.load(inputDsId, LoadOption.SPLIT_COUNT(16))
							.assignUid("uid")
							.splitGeometry(geomCol)
							.drop(0)
							.store(outputDsId)
							.build();

		StopWatch watch = StopWatch.start();
		marmot.createDataSet(outputDsId, ds.getGeometryColumnInfo(), plan, DataSetOption.FORCE);
		watch.stop();
		
		System.out.printf("done: input=%s elapsed=%s%n", inputDsId, watch.getElapsedMillisString());
	}
}
