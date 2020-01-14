package testcase.datastore;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.geo.query.PartitionCache;
import marmot.geo.query.PartitionCache.PartitionKey;
import marmot.geo.query.GeoDataStore;
import marmot.remote.protobuf.PBMarmotClient;
import utils.stream.FStream;
import utils.stream.KeyedGroups;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestCase02 {
	private static final String INPUT = "건물/건물통합정보마스터";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeoDataStore store = GeoDataStore.builder()
										.setMarmotRuntime(marmot)
										.build();

		DataSet ds = marmot.getDataSet(INPUT);
		Envelope range = getGu(marmot, "서초구");
		try ( RecordSet rset = store.createRangeQuery(INPUT, range).run() ) {
			rset.count();
		}
		
		PartitionCache cache = store.getPartitionCache();
		KeyedGroups<String,String> groups = FStream.from(cache.keySet())
													.groupByKey(PartitionKey::getDataSetId,
																PartitionKey::getQuadKey);
		for ( String key: groups.keySet() ) {
			String qkeyList = FStream.from(groups.get(key)).join(", ");
			System.out.printf("%s: { %s }%n", key, qkeyList);
		}
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
