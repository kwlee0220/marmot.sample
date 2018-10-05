package basic;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClient;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleMarmotClient {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClient.getMarmotHost(cl);
		int port = MarmotClient.getMarmotPort(cl);
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		marmot.deleteDataSet("tmp/result");

		
		Plan plan;
		DataSet ds;
	
		// 데이터세트를 읽어 화면에 출력
		//
//		ds = marmot.getDataSet("교통/지하철/서울역사");
//		try ( RecordSet rset = ds.read() ) {
//			rset.stream().limit(5).forEach(System.out::println);
//		}
		
		RecordSet rset;
		
		// Plan을 이용한 데이터 접근
		//
		plan = marmot.planBuilder("test")
					.load("교통/지하철/서울역사")
					.filter("kor_sub_nm.length() > 5")
					.project("sub_sta_sn,kor_sub_nm")
					.build();
//		rset = marmot.executeLocally(plan);
//		SampleUtils.printPrefix(rset, 5);
		
		// 사용자가 제공하는 입력 레코드세트를 활용한 Plan 수행.
		//
		ds = marmot.getDataSet("교통/지하철/서울역사");
		plan = marmot.planBuilder("test2")
					.filter("kor_sub_nm.length() > 5")
					.project("sub_sta_sn,kor_sub_nm")
					.build();
		
		try ( RecordSet input = ds.read() ) {
			try ( RecordSet result = marmot.executeLocally(plan, input) ) {
				SampleUtils.printPrefix(result, 5);
			}
		}
		
		marmot.disconnect();
	}
}
