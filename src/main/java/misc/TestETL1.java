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
public class TestETL1 {
//	private static final String INPUT = "tmp/dtg1";
//	private static final String INPUT = "tmp/dtg2";
//	private static final String INPUT = "tmp/building1";
//	private static final String INPUT = "tmp/building2";
//	private static final String INPUT = "tmp/hospital1";
//	private static final String INPUT = "tmp/hospital2";
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
		
		INPUT = args[0];

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		System.out.println("dataset=" + INPUT);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Plan plan = marmot.planBuilder("test_dtg1")
						.load(INPUT)
						.buffer("the_geom", 50)
						.aggregate(AggregateFunction.ENVELOPE("the_geom"))
//						.expand("the_geom:polygon", "the_geom = ST_GeomFromEnvelope(mbr)")
						.store(RESULT)
						.build();
		DataSet result = marmot.createDataSet(RESULT, plan, DataSetOption.FORCE);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
