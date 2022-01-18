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
public class CreateSampleDataSetOld {
	private static final long COUNT = 12_00_000;
	private static final int STUFF_LEN = 2048;
	private static final long CHUNK_SIZE = 4096;
	private static final String INPUT = "주소/건물POI";
	private static final String SAMPLE = "tmp/sample";
	
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
		
//		createDataSet(marmot, schema, centers, 3);
//		createDataSet(marmot, schema, centers, 5);
//		createDataSet(marmot, schema, centers, 7);
		createDataSet(marmot, schema, centers, 9);
	}
	
	private static final void createDataSet(MarmotRuntime marmot, RecordSchema schema,
											List<Point> centers, int clusterCount) {
		RecordSet input = new RecordPopulator(centers, schema, clusterCount, COUNT);
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		String outDsId = String.format("%s_%d", SAMPLE, clusterCount);
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
		
		RecordPopulator(List<Point> centers, RecordSchema schema, int clusterCount,
						long count) {
			m_schema = schema;
			m_selector = new ClusterSelector(centers, CHUNK_SIZE, clusterCount);
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
		private final long m_clusterCount;
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
		
		ClusterSelector(List<Point> centers, long chunkSize, long clusterCount) {
			m_centers = centers;
			m_chunkSize = chunkSize;
			m_clusterCount = clusterCount;
			m_remains = 0;
		
			m_counts = new long[(int)clusterCount];
			Arrays.fill(m_counts, 0);
			m_rand = new Random(System.currentTimeMillis());
		}
		
		private List<Density> calcDistribution() {
			List<Point> domain = Lists.newArrayList(m_centers);
			List<Density> dist = Lists.newArrayList();
			for ( int i =0; i < m_clusterCount; ++i ) {
				int idx = m_rand.nextInt(domain.size());
				Point center = domain.remove(idx);
				
				double prob = m_rand.nextDouble();
				if ( i % 4 == 0 ) { }
				else if ( i % 4 == 1 ) { prob /= 3; }
				else if ( i % 4 == 2 ) { prob /= 9; }
				else { prob /= 15; }
				dist.add(new Density(dist.size(), center, 0.001 + prob));
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
