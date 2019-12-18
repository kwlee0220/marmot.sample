package oldbldr;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.RecordScript;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step2_Buildings {
	private static final String BUILDINGS = "건물/통합정보";
	private static final String EMD = "구역/읍면동";
	private static final String RESULT = "tmp/oldbld/buildings_emd";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
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
		
		Plan plan;
		
		String init = "$now = DateNow(); $pat = DatePattern('yyyyMMdd');";
		String trans = "$date = null;"
					+ "if ( 사용승인일자 != null && 사용승인일자.length() == 8 ) {"
						+ "if ( 사용승인일자.endsWith('00') ) { 사용승인일자 = 사용승인일자.substring(0,6) + '01' }"
						+ "$date = DateParseLE(사용승인일자, $pat);"
					+ "}"
					+ "else { $date = null; }"
					+ "if ( $date != null ) {"
						+ "$age = (DateDaysBetween($date,$now)) / 365L;"
						+ "old = $age >= 20 ? 1 : 0;"
						+ "be5 = $age >= 5 ? 1 : 0;"
					+ "}"
					+ "else { old = 0; be5 = 0; }";		
		
		plan = Plan.builder("행정구역당 20년 이상된 건물 집계")
					.load(BUILDINGS)
					.defineColumn("emd_cd:string", "pnu.substring(0,8)")
					.expand("old:byte,be5:byte", RecordScript.of(init, trans))
					.project("emd_cd,old,be5")
					.aggregateByGroup(Group.ofKeys("emd_cd").workerCount(1),
										SUM("old").as("old_cnt"), SUM("be5").as("be5_cnt"),
										COUNT().as("bld_cnt"))
					.defineColumn("old_ratio:double", "(double)old_cnt/bld_cnt")
					.store(RESULT, FORCE)
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();
		
		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
