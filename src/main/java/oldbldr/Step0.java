package oldbldr;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
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
public class Step0 {
	private static final String FLOW_POP = "주민/유동인구/월별_시간대/2015";
	private static final String RESULT = "구역/지오비전_집계구pt";
	
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
		
		DataSet info = marmot.getDataSet(FLOW_POP);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Plan plan;
		plan = marmot.planBuilder("2015년도 소지역 점좌표 추출")
					.load(FLOW_POP)
					.project("the_geom,block_cd")
					.distinct("block_cd")
					.store(RESULT)
					.build();
		DataSet result = marmot.createDataSet(RESULT, geomCol, srid, plan, true);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedTimeString());
	}
}
