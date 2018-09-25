package misc;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestETL2 {
//	private static final String INPUT = "tmp/dtg1";
//	private static final String INPUT = "tmp/dtg2";
//	private static final String INPUT = "tmp/building1";
//	private static final String INPUT = "tmp/building2";
//	private static final String INPUT = "tmp/hospital1";
//	private static final String INPUT = "tmp/hospital2";
	private static final String STATIONS = "교통/지하철/출입구";
	private static final String PARAM = "tmp/buffered";
	private static final String RESULT = "tmp/result";
	
	private static String INPUT;
	
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
		
		INPUT = args[0];
		
//		bufferStations(marmot);
		System.out.println("dataset=" + INPUT);
		
		StopWatch watch = StopWatch.start();
		
		Plan plan = marmot.planBuilder("test_dtg")
						.load(INPUT)
						.spatialJoin("the_geom", PARAM, "the_geom,param.sub_sta_sn")
						.groupBy("sub_sta_sn")
							.aggregate(AggregateFunction.COUNT())
						.store(RESULT)
						.build();
		DataSet result = marmot.createDataSet(RESULT, plan, DataSetOption.FORCE);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static void bufferStations(PBMarmotClient marmot) {
		GeometryColumnInfo info = marmot.getDataSet(STATIONS).getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("buffer")
						.load(STATIONS)
						.buffer("the_geom", 100)
						.store(PARAM)
						.build();
		DataSet result = marmot.createDataSet(PARAM, info, plan, DataSetOption.FORCE);
		result.cluster();
	}
}
