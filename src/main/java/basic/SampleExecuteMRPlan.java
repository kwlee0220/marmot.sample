package basic;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExecuteMRPlan {
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
		
		DataSet ds;
		
		Plan plan;
		RecordSet rset;

		//
		// MR을 활용하는 Plan 수행.
		//
		
		// 1. 실행시킬 Plan 객체 생성.
		plan = marmot.planBuilder("test")
					.load("POI/주유소_가격")
					.filter("휘발유 > 1400;")
					.project("상호,휘발유,주소")
					.store("tmp/result")
					.build();
		
		// 2. 결과가 기록될 빈 데이터세트 생성하고, Plan 수행으로 데이터세트에 추가
		DataSet result = marmot.createDataSet("tmp/result", plan, DataSetOption.FORCE);
		
		// 3. Plan 실행 결과 채워진 데이터세트 내용 접근 및 출력
		SampleUtils.printPrefix(result, 5);
		
		marmot.disconnect();
	}
}
