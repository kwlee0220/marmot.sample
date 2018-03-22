package basic;

import java.io.File;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.command.MarmotCommands;
import marmot.geo.geotools.ShapefileRecordSet;
import marmot.geo.geotools.ShapefileRecordSetReader;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleImportShapefile {
	private static final File INPUT = new File("data/test.shp");
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
		
		DataSet ds;
		ShapefileRecordSetReader reader = ShapefileRecordSetReader.from(INPUT)
																.charset("euc-kr");
		try ( ShapefileRecordSet rset = reader.read() ) {
			GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", rset.getSRID());
			ds = marmot.createDataSet(OUTPUT, gcInfo, rset, true);
		}
		watch.stop();

		SampleUtils.printPrefix(ds, 5);
		System.out.printf("import records from %s into %s, elapsed=%s%n",
							INPUT.getAbsolutePath(), ds.getId(),
							watch.stopAndGetElpasedTimeString());
		
		marmot.disconnect();
	}
}
