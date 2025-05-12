package testcase.datastore;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.locationtech.jts.geom.Envelope;

import utils.stream.FStream;
import utils.stream.KeyedGroups;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.geo.query.GeoDataStore;
import marmot.geo.query.PartitionCache;
import marmot.geo.query.PartitionCache.PartitionKey;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestCase03 {
	private static final String INPUT = "건물/건물통합정보마스터";
	private static final String INPUT2 = "구역/집계구";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeoDataStore store = GeoDataStore.builder()
										.setMarmotRuntime(marmot)
										.setParitionCacheExpireSeconds(10)
										.setMaxLocalCacheCost(20)
										.build();

		DataSet ds = marmot.getDataSet(INPUT);
		Envelope range = getGu(marmot, "서초구");
		try ( RecordSet rset = store.createRangeQuery(INPUT, range).run() ) {
			rset.count();
		}
		
		PartitionCache cache = store.getPartitionCache();
		KeyedGroups<String,String> groups = FStream.from(cache.keySet())
													.toKeyValueStream(PartitionKey::getDataSetId,
																	PartitionKey::getQuadKey)
													.groupByKey();
		for ( String key: groups.keySet() ) {
			String qkeyList = FStream.from(groups.get(key)).join(", ");
			System.out.printf("%s: { %s }%n", key, qkeyList);
		}
		
		System.out.println("sleep 13 seconds");
		SECONDS.sleep(13);
		
		range = getSeoChoDongSub(marmot);
		try ( RecordSet rset = store.createRangeQuery(INPUT, range).run() ) {
			rset.count();
		}
	}
	
	private static Envelope getSeoChoDongSub(MarmotRuntime marmot) {
		Plan plan = Plan.builder("get seochodong")
							.load(EMD)
							.filter("emd_cd == 11650108")
							.project("the_geom")
							.build();
		Envelope envl = marmot.executeToGeometry(plan).get().getEnvelopeInternal();
		double width = envl.getWidth() / 32;
		double height = envl.getHeight() / 32;
		return new Envelope(envl.getMinX(), envl.getMinX()+width,
							envl.getMinY(), envl.getMinY()+height);
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
}
