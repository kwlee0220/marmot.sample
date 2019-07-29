package marmot.geom.arc;

import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcClipJoin {
	private static final String INPUT = "안양대/공간연산/clip/input";
	private static final String PARAM = "안양대/공간연산/clip/param";
	private static final String OUTPUT = "안양대/공간연산/clip/output";
	private static final String RESULT = "tmp/result";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("sample_arc_clip")
							.load(INPUT)
							.arcClip("the_geom", PARAM)
							.build();
//		DataSet result = marmot.createDataSet(RESULT, plan, FORCE(gcInfo));
//		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
//		
//		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
//		SampleUtils.printPrefix(result, 5);
		
		check(marmot);
	}
	
	private static void check(MarmotRuntime marmot) {
		DataSet result = marmot.getDataSet(RESULT);
		DataSet output = marmot.getDataSet(OUTPUT);
		
		if ( result.getRecordCount() != output.getRecordCount() ) {
			System.err.printf("count not equal: %d <-> %d%n", result.getRecordCount(), output.getRecordCount());
			return;
		}
		
		Map<String,Double> data;
		try ( RecordSet rset = output.read() ) {
			data = rset.fstream().map(r -> summarize(r))
						.toMap(t -> t._1, t -> t._2);
		}
		
		try ( RecordSet rset = result.read() ) {
			for ( Record r: rset ) {
				Tuple2<String,Double> t = summarize(r);
				Double area = data.remove(t._1);
				if ( area == null ) {
					System.err.println("unknown: id=" + t._1);
					return;
				}
				if ( Math.abs(t._2 - area) >= 0.001 ) {
					System.err.printf("area not equal: %.2f <-> %.2f%n", t._2, area);
					return;
				}
			}
		}
		
//		if ( result.getRecordSchema() != output.getRecordSchema() ) {
//			System.err.printf("incompatiable RecordSchema: %s <-> %s%n",
//								result.getRecordSchema(), output.getRecordSchema());
//			return;
//		}
	}
	
	private static Tuple2<String,Double> summarize(Record record) {
		String id = record.getString("OBJECTID");
		double area = record.getGeometry("the_geom").getLength();
		
		return Tuple.of(id, area);
	}
}
