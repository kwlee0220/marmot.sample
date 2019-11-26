package marmot.advanced;

import java.io.File;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.geo.query.DataSetPartitionCache;
import marmot.geo.query.IndexBasedScan;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleIndexScan {
	private static final String INPUT = "건물/건물통합정보마스터/201809";
	private static final long SAMPLE_COUNT = 5000;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		File cacheDir = new File("/tmp/marmot_geoserver_cache");
		DataSetPartitionCache cache = new DataSetPartitionCache(marmot, cacheDir);
		
		DataSet input = marmot.getDataSet(INPUT);
		Envelope range = getSeoChoGu(marmot);
		
		IndexBasedScan scan = IndexBasedScan.on(input, range, SAMPLE_COUNT, cache, 20);
		try ( RecordSet rset = scan.run() ) {
			long count = rset.count();
			System.out.println("scan count: " + count);
			if ( count > SAMPLE_COUNT ) {
				throw new AssertionError();
			}
		}
	}

	private static final String SGG = "구역/시군구";
	private static Envelope getSeoChoGu(MarmotRuntime marmot) {
		Plan plan = marmot.planBuilder("get seochogu")
							.load(SGG)
							.filter("sig_cd == 11650")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
}
