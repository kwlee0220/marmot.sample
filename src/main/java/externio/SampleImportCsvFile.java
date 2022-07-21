package externio;

import java.io.File;

import utils.StopWatch;

import common.SampleUtils;
import marmot.command.ImportParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.csv.CsvParameters;
import marmot.externio.csv.ImportCsv;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleImportCsvFile {
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
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