package basic;

import java.io.File;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.rset.GeoJsonRecordSet;
import marmot.rset.GeoJsonRecordSetReader;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleImportGeoJson {
	private static final File INPUT = new File("data/test.gjson");
	private static final String OUTPUT = "tmp/result";
	
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
//		KryoMarmotClient marmot = KryoMarmotClient.connect(host, port);
		
		DataSet result;
		GeoJsonRecordSetReader reader = GeoJsonRecordSetReader.from(INPUT)
																.srid("EPSG:5186");
		try ( GeoJsonRecordSet rset = reader.read() ) {
			result = marmot.createDataSet(OUTPUT, "the_geom", rset.getSRID(), rset, true);
		}
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("import records from %s into %s, elapsed=%s%n",
							INPUT.getAbsolutePath(), result.getId(),
							watch.stopAndGetElpasedTimeString());
		
		marmot.disconnect();
	}
}
