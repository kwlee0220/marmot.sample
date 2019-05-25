package appls;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
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
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		
		plan = marmot.planBuilder("pre_process_age_pop")
					.load(INPUT)
					.defineColumn("base_year:int")
					.defineColumn("age_intvl:int", "(item_name.substring(7) / 10) * 10")
					.groupBy("tot_oa_cd,base_year,age_intvl")
						.withTags("the_geom")
						.aggregate(AggregateFunction.SUM("value").as("total"))
					.project("the_geom,tot_oa_cd,base_year,age_intvl,total")
					.groupBy("base_year")
//						.count()
						.storeEachGroup(RESULT, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true))
					.build();
		marmot.execute(plan);
//		marmot.createDataSet(RESULT, plan, DataSetOption.FORCE);
//		marmot.createDataSet(RESULT, plan, DataSetOption.GEOMETRY(gcInfo), DataSetOption.FORCE);
		watch.stop();
		
//		for ( DataSet result: marmot.getDataSetAllInDir(RESULT, true) ) {
//			SampleUtils.printPrefix(result, 5);
//		}
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
