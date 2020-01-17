package testcase.remoteaccess;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.geo.query.GeoDataStore;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestRA06 {
	private static final String INPUT = "건물/건물통합정보마스터";
	private static final String SIDO = "구역/시도";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeoDataStore store = GeoDataStore.builder()
										.setMarmotRuntime(marmot)
										.build();

		Envelope range = getSiDo(marmot, "서울특별시");
		try ( RecordSet rset = store.createRangeQuery(INPUT, range).run() ) {
			long count = rset.count();
			System.out.printf("total count: %d%n", count);
		}
		marmot.close();
	}
	
	private static Envelope getSeoChoDong(MarmotRuntime marmot) {
		Plan plan = Plan.builder("get seochodong")
							.load(EMD)
							.filter("emd_cd == 11650108")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
	
	private static Envelope getSiDo(MarmotRuntime marmot, String name) {
		String expr = String.format("ctp_kor_nm == '%s'", name);
		Plan plan = Plan.builder("get seoul")
							.load(SIDO)
							.filter(expr)
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
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
