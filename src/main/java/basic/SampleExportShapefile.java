package basic;

import java.io.File;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.command.MarmotClient;
import marmot.geo.geotools.ShapefileRecordSetWriter;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleExportShapefile {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final File OUTPUT = new File("data/test.shp");
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		ShapefileRecordSetWriter writer = ShapefileRecordSetWriter.into(OUTPUT)
																	.srid(gcInfo.srid())
																	.charset("euc-kr");
		long ncount = writer.write(ds);
		System.out.printf("written %d records into %s%n",ncount, OUTPUT);
	}
}
