package demo.policy;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.CreateDataSetParameters;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
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
public class Step04 {
	private static final String INPUT = "주민/인구밀도_2000";
	static final String RESULT = "tmp/10min/step04";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("step04 ");
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

		Plan plan = marmot.planBuilder("인구밀도_2017_중심점추출")
							.load(INPUT)
							.centroid(info.name())		// (4) 중심점 추출
							.store(RESULT)
							.build();
		CreateDataSetParameters params = new CreateDataSetParameters(RESULT, plan, true)
																.setGeometryColumnInfo(info)
																.setForce();
		DataSet result = marmot.createDataSet(params);
		System.out.printf("elapsed time=%s (processing)%n", watch.getElapsedMillisString());
		
		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
