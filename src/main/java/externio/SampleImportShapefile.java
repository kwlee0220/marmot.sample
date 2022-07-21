package externio;

import java.io.File;
import java.nio.charset.Charset;

import utils.StopWatch;

import common.SampleUtils;
import marmot.command.ImportParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.shp.ImportShapefile;
import marmot.externio.shp.ShapefileParameters;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleImportShapefile {
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		File file = new File("/mnt/data/sbdata/data/포스웨이브/서울지하철역사");
		ImportParameters importParams = new ImportParameters();
		importParams.setDataSetId("tmp/result");
		importParams.setGeometryColumnInfo("the_geom", "EPSG:4326");
		importParams.setForce(true);
		
		ShapefileParameters shpParams = ShapefileParameters.create()
													.charset(Charset.forName("euc-kr"))
													.srid("EPSG:5186");
		
		ImportIntoDataSet importDs = ImportShapefile.from(file, shpParams, importParams);
		importDs.run(marmot);
		watch.stop();

		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}