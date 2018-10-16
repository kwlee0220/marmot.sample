package marmot.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.SpatialClusterInfo;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleReadSpatialCluster {
	private static final String INPUT = "주소/건물POI";
	private static final String SIDO = "구역/시도";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Envelope range;
		DataSet ds = marmot.getDataSet(INPUT);
		
		range = getGu(marmot, "노원구");
		for ( SpatialClusterInfo info: ds.querySpatialClusterInfo(range) ) {
			String quadKey = info.getQuadKey();
			
			System.out.println(quadKey + ": ");
			try ( RecordSet rset = ds.readSpatialCluster(quadKey, Option.none()) ) {
				SampleUtils.printPrefix(rset, 5);
			}
		}
		
		range = getGu(marmot, "서초구");
		for ( SpatialClusterInfo info: ds.querySpatialClusterInfo(range) ) {
			String quadKey = info.getQuadKey();
			
			System.out.println(quadKey + ": ");
			
			Option<String> filter = Option.some("건물용도분류 == '주택' && 건물명 != null");
			try ( RecordSet rset = ds.readSpatialCluster(quadKey, filter) ) {
				SampleUtils.printPrefix(rset, 5);
			}
		}
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
