package marmot.geom.advanced;

import java.io.File;
import java.io.IOException;

import org.locationtech.jts.geom.Envelope;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.geo.query.RangeQueryEstimate;
import marmot.geo.query.RangeQueryEstimate.ClusterEstimate;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleEstimateRangeQuery {
	private static final String INPUT = "건물/건물통합정보마스터";
//	private static final String INPUT = "교통/지하철/서울역사";
//	private static final String INPUT = "POI/주유소_가격";
	private static final String SIDO = "구역/시도";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		File root = new File("/home/kwlee/tmp/gsp");
		org.apache.commons.io.FileUtils.deleteDirectory(root);
		root.mkdirs();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		test(marmot, "all", getAll(marmot));
		test(marmot, "서울", getSiDo(marmot, "서울특별시"));
		test(marmot, "서초구", getGu(marmot, "서초구"));
		test(marmot, "서초동", getSeoChoDong(marmot));
		test(marmot, "서초동_일부", getSeoChoDongSub(marmot));

		test(marmot, "노원구", getGu(marmot, "노원구"));
		test(marmot, "송파구", getGu(marmot, "송파구"));
		test(marmot, "도봉구", getGu(marmot, "도봉구"));
		test(marmot, "관악구", getGu(marmot, "관악구"));
		test(marmot, "구로구", getGu(marmot, "구로구"));
		test(marmot, "금천구", getGu(marmot, "금천구"));
		test(marmot, "은평구", getGu(marmot, "은평구"));
		test(marmot, "양천구", getGu(marmot, "양천구"));
		test(marmot, "강서구", getGu(marmot, "강서구"));
		test(marmot, "서울특별시", getSiDo(marmot, "서울특별시"));
		test(marmot, "안양시만안구", getGu(marmot, "안양시만안구"));
		test(marmot, "안양시동안구", getGu(marmot, "안양시동안구"));
		test(marmot, "과천시", getGu(marmot, "과천시"));
		test(marmot, "구리시", getGu(marmot, "구리시"));
		test(marmot, "수원시 팔달구", getGu(marmot, "수원시 팔달구"));
		test(marmot, "평택시", getGu(marmot, "평택시"));
		test(marmot, "여주시", getGu(marmot, "여주시"));
		test(marmot, "의정부시", getGu(marmot, "의정부시"));
		test(marmot, "남양주시", getGu(marmot, "남양주시"));
		test(marmot, "시흥시", getGu(marmot, "시흥시"));
		test(marmot, "부천시", getGu(marmot, "부천시"));
		test(marmot, "경기도", getSiDo(marmot, "경기도"));
		test(marmot, "충청남도", getSiDo(marmot, "충청남도"));
		test(marmot, "대전광역시", getSiDo(marmot, "대전광역시"));
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
	
	private static Envelope getAll(MarmotRuntime marmot) {
		return marmot.getDataSet(INPUT).getBounds();
	}
	
	private static Envelope getSeoChoGu(MarmotRuntime marmot) {
		Plan plan = Plan.builder("get seochogu")
							.load(SGG)
							.filter("sig_cd == 11650")
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
	
	private static Envelope getSeoChoDongSub(MarmotRuntime marmot) {
		Plan plan = Plan.builder("get seochodong")
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
