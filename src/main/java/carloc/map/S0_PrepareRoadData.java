package carloc.map;

import org.apache.log4j.PropertyConfigurator;

import carloc.Globals;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S0_PrepareRoadData {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("prepare_road_data ");
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
		
		DataSet input = marmot.getDataSet(Globals.ROADS);
		GeometryColumnInfo info = input.getGeometryColumnInfo();
		String geomCol = info.name();
		
		Plan subPlan = marmot.planBuilder("서브 링크 순차번호 부여")
							.assignUid("sub_link_no")
							.build();
		
		Plan plan;
		plan = marmot.planBuilder("도로 경로 단순화")
					.load(Globals.ROADS)
					.flattenGeometry(geomCol, DataType.LINESTRING)
					.breakLineString(geomCol)
					.groupBy("link_id")
						.run(subPlan)
					.store(Globals.ROADS_IDX)
					.build();
		DataSet result = marmot.createDataSet(Globals.ROADS_IDX, info, plan, true);
//		DataSet result = marmot.getDataSet(Globals.ROADS_IDX);
		System.out.printf("elapsed=%s (simplification)%n", watch.getElapsedMillisString());
		
		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
