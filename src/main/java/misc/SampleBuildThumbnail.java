package misc;


import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.rset.ConcatedRecordSet;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.StopWatch;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleBuildThumbnail {
	private static final String INPUT = "건물/건물통합정보마스터";
	private static final int SAMPLE_SIZE = 1000;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet ds = marmot.getDataSet(INPUT);
		StopWatch watch = StopWatch.start();
		
		try ( RecordSet rset = new RecordSetImpl(marmot, ds, SAMPLE_SIZE) ) {
			long count = rset.fstream()
				.zipWithIndex()
//				.peek(t -> System.out.println(t._2))
				.count();
//				.zipWithIndex()
//				.forEach(t -> {
//					int rank = t._2;
//					Record rec = t._1;
//					
//					String qkey = rec.getString("__quadKey");
//					double ratio = rec.getDouble("__ratio");
//					
//					System.out.printf("%d: %s, %.3f%n", rank, qkey, ratio);
//				});
			
			System.out.printf("%d: elapsed=%s%n", count, watch.getElapsedSecondString());
		}
	}
	
	private static int compare(Record rec1, Record rec2) {
		double area1 = rec1.getGeometry("the_geom").getArea();
		double area2 = rec2.getGeometry("the_geom").getArea();
		
		return Double.compare(area2, area1);
	}
	
	static class RecordSetImpl extends ConcatedRecordSet {
		private DataSet m_source;
		private final RecordSchema m_schema;
		private List<Record> m_entries;
		private final double m_ratio;
		
		RecordSetImpl(MarmotRuntime marmot, DataSet source, long nsamples) {
			m_source = source;
			m_ratio = ((double)SAMPLE_SIZE)/source.getRecordCount();
			
			m_schema = source.getRecordSchema().toBuilder()
							.addColumn("__quadKey", DataType.STRING)
							.addColumn("__ratio", DataType.DOUBLE)
							.build();

			String filterExpr = String.format("Math.round(count * %f) >= 1", m_ratio);
			Plan plan = marmot.planBuilder("test")
							.loadSpatialClusterIndexFile(source.getId())
							.filter(filterExpr)
							.build();
			try ( RecordSet rset = marmot.executeLocally(plan) ) {
				m_entries = rset.toList();
			}
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		@Override
		protected RecordSet loadNext() {
			while ( m_entries.size() > 0 ) {
				Record entry = m_entries.remove(0);
				
				String quadKey = entry.getString("quad_key");
				entry.get("tile_bounds");
				long recCount = entry.getLong("count");
				int topK = (int)Math.round(recCount * m_ratio);
				
				if ( topK > 0 ) {
					try ( RecordSet rset = m_source.readSpatialCluster(quadKey) ) {
						FStream<Record> selecteds = rset.fstream()
							.takeTopK(topK, SampleBuildThumbnail::compare)
							.zipWithIndex()
							.map(t -> attachInfo(t._1, quadKey, topK, t._2));
						return RecordSet.from(m_schema, selecteds);
					}
				}
			}
			
			return null;
		}
		
		private Record attachInfo(Record input, String quadKey, int topK, int rank) {
			Record output = DefaultRecord.of(m_schema);
			output.set(input);
			output.set("__quadKey", quadKey);
			output.set("__ratio", (double)rank / topK);
			
			return output;
		}
	}
}
