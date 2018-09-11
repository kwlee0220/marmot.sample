package carloc.map;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import carloc.Globals;
import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.plan.GeomOpOption;
import marmot.process.geo.EstimateClusterQuadKeysParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S1_MapMatchingTaxiLog2 {
	private static final String INPUT = Globals.TAXI_LOG;
	private static final String QUAD_KEY_FILE = "tmp/qkeys";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("map_matching_taxi_log ");
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
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo info = input.getGeometryColumnInfo();
		String geomCol = info.name();
		
		String script = String.format("%s = ST_ClosestPointOnLine(%s, line)", geomCol, geomCol);
		
		EstimateClusterQuadKeysParameters params = new EstimateClusterQuadKeysParameters();
		params.inputDataset(INPUT);
		params.outputDataset(QUAD_KEY_FILE);
		params.blockSize("32mb");
//		marmot.executeProcess(EstimateClusterQuadKeysParameters.processName(), params.toMap());
		
		List<String> quadKeys;
		try ( RecordSet rset = marmot.getDataSet(QUAD_KEY_FILE).read() ) {
			quadKeys = rset.fstream().map(r -> r.getString(0)).toList();
		}
		finally {
//			marmot.deleteDataSet(QUAD_KEY_FILE);
		}
		int nreducers = Math.max(1, quadKeys.size()/4);
		
		Plan plan;
		plan = marmot.planBuilder("택시로그_맵_매핑")
					.load(INPUT)
					.buffer(geomCol, Globals.DISTANCE, GeomOpOption.OUTPUT("buffer"))
					.attachQuadKey("buffer", "EPSG:5186", quadKeys, true, true)
					.project("*-{buffer,__quad_key,__mbr}, __quad_key as quad_key")
					.groupBy("quad_key")
						.workerCount(nreducers)
						.list()
					.project("*-{quad_key}")
					.knnJoin(geomCol, Globals.ROADS_IDX, 1, Globals.DISTANCE,
							"*,param.{the_geom as link_geom, link_id, sub_link_no}")
					.store("tmp/result")
					.build();
		DataSet result = marmot.createDataSet("tmp/result", info, plan, true);
		watch.stop();

		SampleUtils.printPrefix(result, 10);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
