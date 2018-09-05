package basic;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import marmot.DataSet;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.DefaultRecord;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleGetStream {
	private static final String INPUT = "로그/나비콜/택시로그";
	private static final String INPUT2 = "구역/행정동코드";
	private static final String RESULT = "tmp/result";
	
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
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		StopWatch watch;
		Plan plan;
		
		plan = marmot.planBuilder("range")
					.load(INPUT2)
					.filter("hdong_name == '행궁동'")
					.build();
		Geometry key = marmot.executeLocally(plan).getFirst().get().getGeometry("the_geom");
		
		
		plan = marmot.planBuilder("test SampleGetStream")
							.query(INPUT, SpatialRelation.INTERSECTS, key)
							.build();
		watch = StopWatch.start();
		RecordSet rset = marmot.getStream("test", plan);

		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		Record output = DefaultRecord.of(rset.getRecordSchema());
		rset.next(output);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		while ( rset.next(output) );
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		plan = marmot.planBuilder("xxx")
					.query(INPUT, SpatialRelation.INTERSECTS, key)
					.store(RESULT)
					.build();
		watch = StopWatch.start();
		DataSet result = marmot.createDataSet(RESULT, plan, true);
		try ( RecordSet rset2 = result.read() ) {
			while ( rset.next(output) );
			System.out.printf("elapsed2=%s%n", watch.getElapsedMillisString());
		}
	}
}
