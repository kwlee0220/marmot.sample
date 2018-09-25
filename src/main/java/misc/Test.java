package misc;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
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
public class Test {
	private static final String ADDR_BLD = "건물/위치";
	private static final String ADDR_BLD_UTILS = "tmp/test2017/buildings_utils";
	private static final String GRID = "tmp/test2017/grid30";
	
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
		DataSet result;
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		
		plan = marmot.planBuilder("xx")
					.load("tmp/hcode")
					.update("the_geom = ST_GeomFromEnvelope(ST_AsEnvelope(the_geom))")
					.store("tmp/hcode2")
					.build();
		marmot.createDataSet("tmp/hcode2", gcInfo, plan, DataSetOption.FORCE);
		
		plan = marmot.planBuilder("yy")
					.load("tmp/cada")
					.update("the_geom = ST_GeomFromEnvelope(ST_AsEnvelope(the_geom))")
					.store("tmp/cada2")
					.build();
		marmot.createDataSet("tmp/cada2", gcInfo, plan, DataSetOption.FORCE);

		GeometryColumnInfo gcInfo2 = new GeometryColumnInfo("the_geom", "EPSG:4326");
		plan = marmot.planBuilder("find_closest_point_on_link")
					.load("tmp/cada2")
					.spatialJoin("the_geom", "tmp/hcode2",
								"the_geom,pnu,param.the_geom as the_geom2, param.hcode")
					.expand1("the_geom:point",
							"ST_Centroid(the_geom.intersection(the_geom2))")
					.transformCrs("the_geom", "EPSG:5186", "EPSG:4326", "the_geom")
					.store("tmp/result")
					.build();
		result = marmot.createDataSet("tmp/result", gcInfo2, plan, DataSetOption.FORCE);
		watch.stop();
		
		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
