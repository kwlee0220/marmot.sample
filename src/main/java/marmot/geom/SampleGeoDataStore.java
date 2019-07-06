package marmot.geom;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.geo.query.GeoDataStore;
import marmot.remote.protobuf.PBMarmotClient;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleGeoDataStore {
	private static final String INPUT = "건물/건물통합정보마스터/201809";
	private static final String SIDO = "구역/시도";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeoDataStore store = GeoDataStore.from(marmot).setUsePrefetch(true);
		
		FStream.from(store.getGeoDataSetAll())
				.forEach(System.out::println);
		
		RecordSet rset;
		rset = store.createRangeQuery(INPUT, getGu(marmot, "서초구")).run();
		SampleUtils.printPrefix(rset, 3);
		
		rset = store.createRangeQuery(INPUT, getSeoChoDong(marmot)).run();
		SampleUtils.printPrefix(rset, 3);
		
		rset = store.createRangeQuery(INPUT, getGu(marmot, "노원구")).run();
		SampleUtils.printPrefix(rset, 3);
		
		rset = store.createRangeQuery(INPUT, getGu(marmot, "송파구")).run();
		SampleUtils.printPrefix(rset, 3);
		
		rset = store.createRangeQuery(INPUT, getGu(marmot, "양천구")).run();
		SampleUtils.printPrefix(rset, 3);
		
		rset = store.createRangeQuery(INPUT, getGu(marmot, "관악구")).run();
		SampleUtils.printPrefix(rset, 3);
		
		rset = store.createRangeQuery(INPUT, getSiDo(marmot, "서울특별시")).run();
		SampleUtils.printPrefix(rset, 3);
		
		rset = store.createRangeQuery(INPUT, getSiDo(marmot, "경상남도")).run();
		SampleUtils.printPrefix(rset, 3);
	}
	
	private static Envelope getAll(MarmotRuntime marmot) {
		return marmot.getDataSet(INPUT).getBounds();
	}
	
	private static Envelope getSeoChoGu(MarmotRuntime marmot) {
		Plan plan = marmot.planBuilder("get seochogu")
							.load(SGG)
							.filter("sig_cd == 11650")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
	
	private static Envelope getSeoChoDong(MarmotRuntime marmot) {
		Plan plan = marmot.planBuilder("get seochodong")
							.load(EMD)
							.filter("emd_cd == 11650108")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
	
	private static Envelope getSeoChoDongSub(MarmotRuntime marmot) {
		Plan plan = marmot.planBuilder("get seochodong")
							.load(EMD)
							.filter("emd_cd == 11650108")
							.project("the_geom")
							.build();
		Envelope envl = marmot.executeToGeometry(plan).get().getEnvelopeInternal();
		double width = envl.getWidth() / 4;
		double height = envl.getHeight() / 4;
		return new Envelope(envl.getMinX(), envl.getMinX()+width,
							envl.getMinY(), envl.getMinY()+height);
	}
	
	private static Envelope getSiDo(MarmotRuntime marmot, String name) {
		String expr = String.format("ctp_kor_nm == '%s'", name);
		Plan plan = marmot.planBuilder("get seoul")
							.load(SIDO)
							.filter(expr)
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
	
	private static Envelope getGu(MarmotRuntime marmot, String guName) {
		String expr = String.format("sig_kor_nm == '%s'", guName);
		Plan plan = marmot.planBuilder("get seochodong")
							.load(SGG)
							.filter(expr)
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
}
