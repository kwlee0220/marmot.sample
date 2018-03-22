package basic;

import java.io.File;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.command.MarmotCommands;
import marmot.geo.geotools.ShapefileRecordSetWriter;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExportShapefile {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final File OUTPUT = new File("data/test.shp");
	
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
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		ShapefileRecordSetWriter writer = ShapefileRecordSetWriter.into(OUTPUT)
																	.srid(gcInfo.srid())
																	.charset("euc-kr");
		long ncount = writer.write(ds);
		watch.stop();
		
		System.out.printf("written %d records into %s, elapsed=%s%n",
							ncount, OUTPUT, watch.getElapsedTimeString());
	}
}
