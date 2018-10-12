package externio;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.command.MarmotClientCommands;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.ImportParameters;
import marmot.externio.shp.ImportShapefile;
import marmot.externio.shp.ShapefileParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleImportShapefile {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
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
		
		File file = new File("/mnt/data/sbdata/data/포스웨이브/서울지하철역사");
		ImportParameters importParams = ImportParameters.create()
													.setDatasetId("tmp/result")
													.setGeometryColumnInfo("the_geom", "EPSG:4326")
													.setForce(true);
		ShapefileParameters shpParams = ShapefileParameters.create()
													.charset(Charset.forName("euc-kr"))
													.shpSrid("EPSG:5186");
		
		ImportIntoDataSet importDs = ImportShapefile.from(file, shpParams, importParams);
		importDs.run(marmot);
		watch.stop();

		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}