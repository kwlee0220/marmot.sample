package basic;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.command.MarmotClient;
import marmot.externio.geojson.GeoJsonRecordSetWriter;
import marmot.remote.protobuf.PBMarmotClient;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExportGeoJson {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String OUTPUT = "data/test.gjson";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();
		
		DataSet ds = marmot.getDataSet(INPUT);
		try ( GeoJsonRecordSetWriter writer = GeoJsonRecordSetWriter.get(new File(OUTPUT),
																		StandardCharsets.UTF_8)
																	.prettyPrinter(true) ) {
			long ncount = writer.write(ds);
			System.out.printf("written %d records into %s%n", ncount, OUTPUT);
		}
	}
}
