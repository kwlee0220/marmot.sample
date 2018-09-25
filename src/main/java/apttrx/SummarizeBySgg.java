package apttrx;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.DataSetOption;
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
public class SummarizeBySgg {
	private static final String APT_LOC = "주택/실거래/아파트위치";
	private static final String APT_TRX = "주택/실거래/아파트매매";
	private static final String SGG = "구역/시군구";
	private static final String RESULT = "tmp/아파트실매매/시군구별";
	
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

		Plan plan;
		DataSet emd = marmot.getDataSet(SGG);
		String geomCol = emd.getGeometryColumn();
		
		plan = marmot.planBuilder("summarize_by_station")
						.load(APT_TRX)
						.join("시군구,번지,단지명", APT_LOC, "시군구,번지,단지명", "*,param.{info}", null)
						.expand("평당거래액:int,sgg_cd:string",
								"평당거래액 = (int)Math.round((거래금액*3.3) / 전용면적);"
										+ "sgg_cd = info.getSggCode();")
						.groupBy("sgg_cd")
							.aggregate(COUNT().as("거래건수"),
										SUM("거래금액").as("총거래액"),
										AVG("평당거래액").as("평당거래액"),
										MAX("거래금액").as("최대거래액"),
										MIN("거래금액").as("최소거래액"))
						.expand1("평당거래액:int")
						
						.join("sgg_cd", SGG, "sig_cd",
								String.format("*,param.{%s,sig_kor_nm}", geomCol), null)
						.sort("거래건수:D")
						
						.store(RESULT)
						.build();
		marmot.createDataSet(RESULT, emd.getGeometryColumnInfo(), plan, DataSetOption.FORCE);
		watch.stop();
		
		System.out.printf("elapsed: %s%n", watch.getElapsedMillisString());
	}
}
