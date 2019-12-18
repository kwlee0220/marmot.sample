package marmot.geom;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.geo.query.GeoDataStore;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleGeoDataStore {
	private static final String INPUT = "건물/건물통합정보마스터/201809";
//	private static final String SIDO = "구역/시도";
//	private static final String SGG = "구역/시군구";
//	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeoDataStore store = GeoDataStore.from(marmot).setUsePrefetch(true);
		
		for ( DataSet ds: store.getGeoDataSetAll() ) {
			System.out.println(ds);
		}
		
		RecordSet rset;
		Envelope bounds;
		
		// 서초구 
		bounds = new Envelope(198216.9927942209, 208471.44085357513,
								536547.4066811937, 547344.8814057615);
		rset = store.createRangeQuery(INPUT, bounds).run();
		SampleUtils.printPrefix(rset, 3);
		
		// 노원구 
		bounds = new Envelope(203677.14832563806, 209934.18424544204,
								557188.3157193642, 566276.2981287376);
		rset = store.createRangeQuery(INPUT, bounds).run();
		SampleUtils.printPrefix(rset, 3);
		
		// 송파구 
		bounds = new Envelope(205966.85735437565, 214274.61621185002,
								540795.2117468617, 549301.5654773772);
		rset = store.createRangeQuery(INPUT, bounds).run();
		SampleUtils.printPrefix(rset, 3);
		
		// 양천구 
		bounds = new Envelope(184232.41874555396, 190337.790005593,
								544835.3437955058, 550199.767873937);
		rset = store.createRangeQuery(INPUT, bounds).run();
		SampleUtils.printPrefix(rset, 3);
		
		// 관악구 
		bounds = new Envelope(191061.54995567014, 198994.55364876823,
								537370.740636796, 543959.5132873337);
		rset = store.createRangeQuery(INPUT, bounds).run();
		SampleUtils.printPrefix(rset, 3);
		
		// 서울특별시 
		bounds = new Envelope(179189.76162216233, 216242.27243390775,
								536547.406706411, 566863.5428850177);
		rset = store.createRangeQuery(INPUT, bounds).run();
		SampleUtils.printPrefix(rset, 3);
		
		// 경상남도 
		bounds = new Envelope(252392.16240766164, 401581.85085736896,
								212382.3059561663, 368410.9210480412);
		rset = store.createRangeQuery(INPUT, bounds).run();
		SampleUtils.printPrefix(rset, 3);
	}
	
//	private static Envelope getAll(MarmotRuntime marmot) {
//		return marmot.getDataSet(INPUT).getBounds();
//	}
//	
//	private static Envelope getSeoChoGu(MarmotRuntime marmot) {
//		Plan plan = Plan.builder("get seochogu")
//							.load(SGG)
//							.filter("sig_cd == 11650")
//							.project("the_geom")
//							.build();
//		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
//	}
//	
//	private static Envelope getSeoChoDong(MarmotRuntime marmot) {
//		Plan plan = Plan.builder("get seochodong")
//							.load(EMD)
//							.filter("emd_cd == 11650108")
//							.project("the_geom")
//							.build();
//		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
//	}
//	
//	private static Envelope getSeoChoDongSub(MarmotRuntime marmot) {
//		Plan plan = Plan.builder("get seochodong")
//							.load(EMD)
//							.filter("emd_cd == 11650108")
//							.project("the_geom")
//							.build();
//		Envelope envl = marmot.executeToGeometry(plan).get().getEnvelopeInternal();
//		double width = envl.getWidth() / 4;
//		double height = envl.getHeight() / 4;
//		return new Envelope(envl.getMinX(), envl.getMinX()+width,
//							envl.getMinY(), envl.getMinY()+height);
//	}
//	
//	private static Envelope getSiDo(MarmotRuntime marmot, String name) {
//		String expr = String.format("ctp_kor_nm == '%s'", name);
//		Plan plan = Plan.builder("get seoul")
//							.load(SIDO)
//							.filter(expr)
//							.project("the_geom")
//							.build();
//		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
//	}
//	
//	private static Envelope getGu(MarmotRuntime marmot, String guName) {
//		String expr = String.format("sig_kor_nm == '%s'", guName);
//		Plan plan = Plan.builder("get seochodong")
//							.load(SGG)
//							.filter(expr)
//							.project("the_geom")
//							.build();
//		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
//	}
}
