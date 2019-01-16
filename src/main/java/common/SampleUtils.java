package common;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import java.io.File;
import java.util.Map;

import org.apache.commons.lang.SystemUtils;

import com.google.common.collect.Maps;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleUtils {
	private static final File TEMP_DIR;
	static {
		if ( SystemUtils.IS_OS_WINDOWS ) {
			TEMP_DIR = new File("C:\\Temp");
		}
		else if ( SystemUtils.IS_OS_LINUX ) {
			TEMP_DIR = new File(new File(System.getProperty("user.home")), "tmp");
		}
		else {
			TEMP_DIR = new File(".");
		}
	}
	
	public static DataSet writeSeoul(MarmotRuntime marmot, String dsId) {
		GeometryColumnInfo gcInfo = marmot.getDataSet("구역/시도").getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("extract_seoul")
						.load("구역/시도")
						.filter("ctprvn_cd == '11'")
						.build();
		return marmot.createDataSet(dsId, plan, GEOMETRY(gcInfo), FORCE);
	}
	
	public static void printPrefix(DataSet dataset, int count) {
		try ( RecordSet rset = dataset.read() ) {
			printPrefix(rset, count);
		}
	}
	
	public static void printPrefix(RecordSet rset, int count) {
		RecordSchema schema = rset.getRecordSchema();
		Record record = DefaultRecord.of(schema);
		int[] colIdxs = schema.getColumnAll().stream()
							.filter(c -> !c.type().isGeometryType())
							.mapToInt(c -> c.ordinal())
							.toArray();
		
		int i = 0;
		try {
			while ( ++i <= count && rset.next(record) ) {
				Map<String,Object> values = Maps.newLinkedHashMap();
				for ( int j =0; j < colIdxs.length; ++j ) {
					String name = schema.getColumnAt(colIdxs[j]).name().get();
					Object value = record.get(colIdxs[j]);
					values.put(name, value);
				}
				System.out.println(values);
			}
		}
		finally {
			rset.closeQuietly();
		}
	}
}
