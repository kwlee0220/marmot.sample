package basic;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSetOption;
import marmot.Plan;
import marmot.PlanExecution;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAsyncExecutePlan {
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

		marmot.deleteDataSet("tmp/result");

		// 1. 실행시킬 Plan 객체 생성.
		Plan plan = marmot.planBuilder("test")
							.load("POI/주유소_가격")
							.filter("휘발유 > 1400;")
							.project("상호,휘발유,주소")
							.store("tmp/result")
							.build();
		
		// 2. Plan실행 결과로 생성될 레코드세트의 스키마 계산하고
		//		해당 스키마를 갖는 데이터세트를 생성한다.
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		marmot.createDataSet("tmp/result", schema, DataSetOption.FORCE);
		
		// 3. 주어진 Plan을 실행하는 실행 객체를 생성한다.
		//		(이때까지도 아직 Plan을 실행되지 않음)
		PlanExecution exec = marmot.createPlanExecution(plan);
		
		// 4. Plan 실행객체를 통해 plan을 실행시킨다.
		exec.start();
		exec.waitForStarted();
		System.out.printf("is_stated=%s (should be true)%n", exec.isStarted());
		
		// 5. Plan 실행이 종료될 때까지 대기한다.
		exec.waitForDone();
		System.out.printf("done=%s (should be true)%n", exec.isDone());
		
		System.out.println(exec.waitForResult());
		
		marmot.disconnect();
	}
}
