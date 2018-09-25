package debs;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.optor.AggregateFunction;
import marmot.plan.RecordScript;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.DimensionDouble;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TrimTrackLog {
	private static final String INPUT = "debs/ship_tracks";
	private static final int RESOLUTION_LAT = 256;
	private static final int RESOLUTION_LON = 512;
	private static final String RESULT = "tmp/debs/fishnet";
	
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
		
		DataSet ds = marmot.getDataSet(INPUT);
		
		Envelope bounds = bindBorder(marmot);
		DimensionDouble cellSize = new DimensionDouble(bounds.getWidth() / RESOLUTION_LON,
														bounds.getHeight() / RESOLUTION_LAT);
		
		String initExpr = "$pattern = ST_DTPattern('dd-MM-yy hh:mm:ss')";
		String expr = "ts = ST_DTToMillis(ST_DTParseLE(timestamp, $pattern))";
		
		Plan plan = marmot.planBuilder("trim_log")
							.load(INPUT)
							.expand("ts:long", RecordScript.of(initExpr, expr))
							.project("the_geom,ship_id,depart_port,ts")
//							.groupBy("depart_port,ship_id")
//								.reduce(reducer)
//							
//							.expand("count:int", "count = 1")
//							.groupBy("ship_id,departure_port_name, cell_id")
//								.taggedKeyColumns("cell_geom")
//								.workerCount(11)
//								.aggregate(AggregateFunction.SUM("count").as("count"))
//							.project("cell_geom as the_geom,ship_id,departure_port_name,count")
							.store(RESULT)
							.build();
		DataSet result = marmot.createDataSet(RESULT, ds.getGeometryColumnInfo(), plan, DataSetOption.FORCE);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static final Envelope bindBorder(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("find_mbr")
						.load(INPUT)
						.aggregate(AggregateFunction.ENVELOPE("the_geom").as("the_geom"))
						.store("tmp/ship_tracks_mbr")
						.build();
		DataSet result = marmot.createDataSet("tmp/ship_tracks_mbr", plan, DataSetOption.FORCE);
		try ( RecordSet rset = result.read() ) {
			return (Envelope)rset.fstream()
								.first()
								.map(r -> r.get(0))
								.getOrNull();
		}
	}
}
