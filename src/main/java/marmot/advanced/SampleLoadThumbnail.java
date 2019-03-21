package marmot.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadThumbnail {
//	private static final String INPUT = "건물/건물통합정보마스터";
	private static final String INPUT = "주소/건물POI";
	private static final int SAMPLE_SIZE = 50000;
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan;
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		Envelope range = input.getBounds();
				
//		range = shrink(range);
//		range = shrink(range);
//		range = shrink(range);
//		range = shrink(range);
//		range = shrink(range);
//		range = shrink(range);
//		range = shrink(range);
//		range = shrink(range);
//		range = shrink(range);

//		range = new Envelope(297433.0066011658, 297490.56805691094, 423413.51341996575, 423468.0927846073);
		
		print(input, range);
	}
	
	private static void print(DataSet ds, Envelope range) {
		try ( RecordSet rset = ds.readThumbnail(range, SAMPLE_SIZE) ) {
			System.out.println("count = " + rset.count());
		}
	}
	
	private static Envelope shrink(Envelope range) {
		double w = range.getWidth() / 4;
		double h = range.getHeight() / 4;
		return new Envelope(range.getMinX() + w, range.getMinX() + (3*w),
				 			range.getMinY() + h, range.getMinY() + (3*h));
	}
}
