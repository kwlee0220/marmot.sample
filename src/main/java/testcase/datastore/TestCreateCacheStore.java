package testcase.datastore;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import org.locationtech.jts.geom.Envelope;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.geo.query.GeoDataStore;
import marmot.geo.query.RangeQueryEstimate;
import marmot.geo.query.RangeQueryEstimate.ClusterEstimate;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestCreateCacheStore {
	private static final String INPUT = "건물/건물통합정보마스터/201809";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeoDataStore store = GeoDataStore.builder()
										.setMarmotRuntime(marmot)
										.build();
		
		for ( DataSet ds: store.getGeoDataSetAll() ) {
			System.out.println(ds);
		}

		DataSet ds = marmot.getDataSet(INPUT);
		Envelope range = getSeoChoDong(marmot);
		RangeQueryEstimate est = ds.estimateRangeQuery(range);
		for ( ClusterEstimate clusterEst: est.getClusterEstimates() ) {
			String qkey = clusterEst.getQuadKey();
			RecordSet rset = ds.readSpatialCluster(qkey);
		}
	}
	
	private static void test(MarmotRuntime marmot, String title, Envelope range) throws IOException {
		DataSet ds = marmot.getDataSet(INPUT);
		
		RangeQueryEstimate est = ds.estimateRangeQuery(range);
		System.out.println(title + ": total matches=" + est.getMatchCount());
		for ( ClusterEstimate clusterEst: est.getClusterEstimates() ) {
			System.out.println("\t" + clusterEst);
		}
		System.out.println("------------------------------------------------");
	}
	
	private static Envelope getGu(MarmotRuntime marmot, String guName) {
		String expr = String.format("sig_kor_nm == '%s'", guName);
		Plan plan = Plan.builder("get seochodong")
							.load(SGG)
							.filter(expr)
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
	
	private static Envelope getSeoChoDong(MarmotRuntime marmot) {
		Plan plan = Plan.builder("get seochodong")
							.load(EMD)
							.filter("emd_cd == 11650108")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
}
