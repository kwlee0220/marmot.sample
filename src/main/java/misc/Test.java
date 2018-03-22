package misc;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
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

		DataSet input = marmot.getDataSet("교통/지하철/출입구");
		String m_inGeomCol = input.getGeometryColumn();
		String srid = input.getGeometryColumnInfo().srid();
		
		String paramId = "교통/도로/네트워크_추진단";
		DataSet m_paramDs = marmot.getDataSet(paramId);
		String joinCols = String.format("the_geom,param.the_geom as the_geom2,param.*-{%s}",
										m_paramDs.getGeometryColumn());
		
		String updateExpr = "the_geom = ST_GetPerpendicularFoot(the_geom, the_geom2);\n"
							+ "src_geom = the_geom;\n"
							+ "length = ST_Distance(src_geom, dst_geom);";

		Plan plan;
		plan = marmot.planBuilder("find_closest_point_on_link")
					.load("교통/지하철/출입구")
					.knnJoin(m_inGeomCol, paramId, 2, 10, joinCols)
					.expand("length:double", updateExpr)
					.project("the_geom, id as link_id, length, "
							+ "src_node as begin_node, src_geom as begin_geom, "
							+ "dst_node as end_node, dst_geom as end_geom")
					
					.expand("remains:double", "remains = 1000 - length")
					.store("tmp/result")
					.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", srid);
		DataSet result = marmot.createDataSet("tmp/result", gcInfo, plan, true);
		watch.stop();
		
		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedTimeString());
	}
}
