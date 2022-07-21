package marmot.geom;

import java.io.IOException;
import java.util.List;

import org.locationtech.jts.geom.Envelope;

import utils.stream.FStream;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.geo.query.RangeQueryEstimate.ClusterEstimate;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleQuerySpatialClusterInfo {
	private static final String INPUT = "주소/건물POI";
	private static final String SIDO = "구역/시도";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet ds = marmot.getDataSet(INPUT);
		
		test(ds, "서초구", getGu(marmot, "서초구"));
		test(ds, "노원구", getGu(marmot, "노원구"));
		test(ds, "송파구", getGu(marmot, "송파구"));
		test(ds, "서울특별시", getSiDo(marmot, "서울특별시"));
	}
	
	private static void test(DataSet ds, String title, Envelope range) throws IOException {
		List<String> quadKeys = FStream.from(ds.estimateRangeQuery(range).getClusterEstimates())
									.map(ClusterEstimate::getQuadKey)
									.toList();
		System.out.print(title + ": ");
		System.out.println(quadKeys);
		System.out.println("------------------------------------------------");
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
