package appls;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PreProcessAgePop {
	private static final String INPUT = "주민/성연령별인구";
	private static final String RESULT = "분석결과/5차년도_통합시연/연령대별_인구";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
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
		DataSet result;
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		
		plan = marmot.planBuilder("pre_process_age_pop")
					.load(INPUT)
					.expand1("base_year:int")
					.expand1("age_intvl:int", "(item_name.substring(7) / 10) * 10")
					.groupBy("tot_oa_cd,base_year,age_intvl")
						.tagWith("the_geom")
						.aggregate(AggregateFunction.SUM("value").as("total"))
					.project("the_geom,tot_oa_cd,base_year,age_intvl,total")
					.store(RESULT)
					.build();
		result = marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
