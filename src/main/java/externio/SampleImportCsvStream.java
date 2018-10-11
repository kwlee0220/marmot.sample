package externio;

import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotClient;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.ImportParameters;
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
public class SampleImportCsvStream {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClient.getMarmotHost(cl);
		int port = MarmotClient.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		File file = new File("/mnt/data/sbdata/data/공공데이터포털/주유소_가격/주유소_가격.csv");
		CsvParameters params = CsvParameters.create()
											.delimiter('|')
											.headerFirst(true)
											.pointColumn("경도|위도")
											.csvSrid("EPSG:4326");
		ImportParameters importParams = ImportParameters.create()
													.setDatasetId("tmp/result")
													.setGeometryColumnInfo("the_geom", "EPSG:5186")
													.setForce(true);
		Plan plan = marmot.planBuilder("import_plan")
							.project("the_geom,고유번호,휘발유")
							.build();
		
		BufferedReader reader = Files.newBufferedReader(file.toPath());
		ImportIntoDataSet importDs = ImportCsv.from(reader, plan, params, importParams);
		importDs.run(marmot);
		watch.stop();

		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}