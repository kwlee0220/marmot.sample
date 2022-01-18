package misc.perf.join;

import static marmot.optor.CreateDataSetOptions.FORCE;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Lists;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.GeoClientUtils;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.rset.AbstractRecordSet;
import marmot.support.Range;
import marmot.type.DataType;
import utils.func.FOption;
import utils.stream.FStream;
import utils.stream.LongFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateSampleDataSet {
	private static final long COUNT = 900_000;
	private static final int STUFF_LEN = 2048;
	private static final long CHUNK_SIZE = 4096;
	private static final String INPUT = "주소/건물POI";
	private static final String SAMPLE = "tmp/sample";
	
//	private static final double[] DIST_2 = {0.8, 0.2};
//	private static final double[] DIST_4 = {0.65, 0.2, 0.1, 0.05};
//	private static final double[] DIST_6 = {0.5, 0.20, 0.14, 0.1, 0.05, 0.01};
//	private static final double[] DIST_8 = {0.4, 0.2, 0.12, 0.09, 0.05, 0.02, 0.01, 0.01};
//	private static final double[] DIST_10 = {0.32, 0.19, 0.14, 0.09, 0.08, 0.07, 0.05, 0.03, 0.02, 0.01};
	
//	private static final double[] DIST_2 = {0.8, 0.2};
//	private static final double[] DIST_4 = {0.60, 0.2, 0.1, 0.1};
//	private static final double[] DIST_6 = {0.44, 0.20, 0.14, 0.1, 0.07, 0.05};
//	private static final double[] DIST_8 = {0.32, 0.15, 0.15, 0.1, 0.1, 0.08, 0.05, 0.05};
//	private static final double[] DIST_10 = {0.30, 0.15, 0.13, 0.11, 0.1, 0.07, 0.05, 0.05, 0.03, 0.01};

	private static final double[] DIST_1 = {1};
	private static final double[] DIST_3 = {0.50, 0.3, 0.2};
	private static final double[] DIST_5 = {0.50, 0.20, 0.15, 0.1, 0.05};
	private static final double[] DIST_7 = {0.37, 0.21, 0.15, 0.1, 0.1, 0.05, 0.02};
	private static final double[] DIST_9 = {0.33, 0.15, 0.13, 0.11, 0.1, 0.07, 0.05, 0.05, 0.01};
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		List<Point> centers = findPartitionCenters(marmot);
		
		// 생성될 데이터세트의 스키마를 정의함.
		RecordSchema schema = RecordSchema.builder()
										.addColumn("the_geom", DataType.POLYGON)
										.addColumn("stuff", DataType.STRING)
										.build();
		
		createDataSet(marmot, schema, centers, DIST_1);
		createDataSet(marmot, schema, centers, DIST_3);
		createDataSet(marmot, schema, centers, DIST_5);
		createDataSet(marmot, schema, centers, DIST_7);
		createDataSet(marmot, schema, centers, DIST_9);
	}
	
	private static final void createDataSet(MarmotRuntime marmot, RecordSchema schema,
											List<Point> centers, double[] dist) {
		RecordSet input = new RecordPopulator(centers, schema, dist, COUNT);
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		String outDsId = String.format("%s_%d", SAMPLE, dist.length);
		DataSet ds = marmot.createDataSet(outDsId, input.getRecordSchema(), FORCE(gcInfo));
		ds.append(input);
		
		System.out.printf("created dataset: %s%n", outDsId);
	}
	
	private static final List<Point> findPartitionCenters(MarmotRuntime marmot) {
		Plan plan;
		plan = Plan.builder("list partitions")
					.loadSpatialClusterIndexFile(INPUT)
					.defineColumn("center:point", "ST_Centroid(tile_bounds)")
					.project("center")
					.build();
		return marmot.executeLocally(plan)
					.fstream()
					.map(r -> r.getGeometry(0))
					.cast(Point.class)
					.toList();
	}
	
	private static class RecordPopulator extends AbstractRecordSet {
		private final RecordSchema m_schema;
		private final ClusterSelector m_selector;
		private final String m_dummy;
		private long m_remains;
		
		RecordPopulator(List<Point> centers, RecordSchema schema, double[] dist,
						long count) {
			m_schema = schema;
			m_selector = new ClusterSelector(centers, CHUNK_SIZE, dist);
			m_remains = count;
			
			m_dummy = FStream.range(0, STUFF_LEN)
							.map(idx -> (int)'A' + (idx % 26))
							.map(idx -> (char)(int)idx)
							.join("");
							
		}
		
		@Override protected void closeInGuard() throws Exception {
			m_selector.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			if ( m_remains > 0 ) {
				Point center = m_selector.next().get();
				Envelope envl = center.getEnvelopeInternal();
				envl.expandBy(10);
				Polygon poly = GeoClientUtils.toPolygon(envl);
				
				output.set(0, poly);
				output.set(1, m_dummy);
				--m_remains;
				
				return true;
			}
			else {
				return false;
			}
		}
	}
	
	private static class ClusterSelector implements FStream<Point> {
		private final List<Point> m_centers;
		private final long m_chunkSize;
		private final double[] m_dist;;
		private final Random m_rand;
		
		private List<Density> m_distribution;
		private long m_remains;
		private long[] m_counts;
		
		private static class Density {
			private final int m_idx;
			private final Point m_center;
			private double m_ratio;
			private Range<Double> m_range;
			
			Density(int idx, Point center, double ratio) {
				m_idx = idx;
				m_center = center;
				m_ratio = ratio;
			}
		}
		
		ClusterSelector(List<Point> centers, long chunkSize, double[] dist) {
			m_centers = centers;
			m_chunkSize = chunkSize;
			m_dist = dist;
			m_remains = 0;
		
			m_counts = new long[(int)m_dist.length];
			Arrays.fill(m_counts, 0);
			m_rand = new Random(System.currentTimeMillis());
		}
		
		private List<Density> calcDistribution() {
			List<Point> domain = Lists.newArrayList(m_centers);
			List<Density> dist = Lists.newArrayList();
			for ( int i =0; i < m_dist.length; ++i ) {
				int idx = m_rand.nextInt(domain.size());
				Point center = domain.remove(idx);
				
				double prob = m_dist[i];
				dist.add(new Density(dist.size(), center, prob));
			}
			
			double sum = FStream.from(dist).mapToDouble(d -> d.m_ratio).sum();
			FStream.from(dist).forEach(d -> d.m_ratio = d.m_ratio / sum);
			FStream.from(dist).foldLeft(0d, (a,d) -> {
				double upper = a + d.m_ratio;
				d.m_range = Range.of(a, upper);
				return upper;
			});
			
			return dist;
		}

		@Override public void close() throws Exception {
			long total = LongFStream.of(m_counts).sum();
			String[] ratios = LongFStream.of(m_counts)
										.mapToObj(c -> String.format("%.2f", (double)c / total))
										.toArray(String.class);
			
			System.out.println(Arrays.toString(ratios));
		}

		@Override
		public FOption<Point> next() {
			if ( m_remains <= 0 ) {
				m_distribution = calcDistribution();
				m_remains = m_chunkSize;
			}
			
			--m_remains;
			double v = m_rand.nextDouble();
			Density density = FStream.from(m_distribution)
									.findFirst(d -> d.m_range.contains(v))
									.get();
			++m_counts[density.m_idx];
			return FOption.of(density.m_center);
		}
	}
}
