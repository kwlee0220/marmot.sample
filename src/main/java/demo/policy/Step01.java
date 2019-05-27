package demo.policy;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.ExecutePlanOptions;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step01 {
	private static final String INPUT = "POI/노인복지시설";
	static final String RESULT = "tmp/10min/step01";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("step01 ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("노인복지시설_경로당_추출")
						.load(INPUT)
						.filter("induty_nm == '경로당'")			// (1) 영역분석
						.store(RESULT)
						.build();
		DataSet result = marmot.createDataSet(RESULT, plan, ExecutePlanOptions.create().disableLocalExecution(true),
												StoreDataSetOptions.create().geometryColumnInfo(info).force(true));
		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
