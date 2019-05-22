package externio;

import java.io.File;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.command.ImportParameters;
import marmot.command.MarmotClientCommands;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.csv.CsvParameters;
import marmot.externio.csv.ImportCsv;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleImportCsvFile {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("sample_import_csv ");
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
		
		File file = new File("/mnt/data/sbdata/data/공공데이터포털/주유소_가격/주유소_가격.csv");
		CsvParameters csvOpts = CsvParameters.create()
										.delimiter('|')
										.headerFirst(true)
										.pointColumns("경도|위도")
										.srid("EPSG:4326");
		ImportParameters importParams = new ImportParameters();
		importParams.setDataSetId("tmp/result");
		importParams.setGeometryColumnInfo("the_geom", "EPSG:5186");
		importParams.setForce(true);
		
		ImportIntoDataSet importDs = ImportCsv.from(file, csvOpts, importParams);
		importDs.run(marmot);
		watch.stop();

		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}