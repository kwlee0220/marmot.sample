package externio;

import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.ImportParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.geojson.GeoJsonParameters;
import marmot.externio.geojson.ImportGeoJson;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleImportGeoJsonStream {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		File file = new File("/mnt/data/sbdata/data/기타/유엔진/읍면동/읍면동.geojson");
		GeoJsonParameters params = GeoJsonParameters.create()
											.geoJsonSrid("EPSG:4326");
		ImportParameters importParams = new ImportParameters();
		importParams.setDataSetId("tmp/result");
		importParams.setGeometryColumnInfo("the_geom", "EPSG:5186");
		importParams.setForce(true);
		
		Plan plan = Plan.builder("import_plan")
							.project("the_geom,emd_kor_name")
							.build();
		
		BufferedReader reader = Files.newBufferedReader(file.toPath());
		ImportIntoDataSet importDs = ImportGeoJson.from(reader, plan, params, importParams);
		importDs.run(marmot);
		watch.stop();
		
		DataSet result = marmot.getDataSet("tmp/result");
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}