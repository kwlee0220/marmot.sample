package marmot.geom;

import java.io.InputStream;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.SpatialClusterInfo;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.rset.PBInputStreamRecordSet;

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
			try ( InputStream is = ds.readRawSpatialCluster(quadKey);
					RecordSet rset = PBInputStreamRecordSet.from(is) ) {
				SampleUtils.printPrefix(rset, 5);
			}
		}
		
		range = getGu(marmot, "서초구");
		for ( SpatialClusterInfo info: ds.querySpatialClusterInfo(range) ) {
			String quadKey = info.getQuadKey();
			
			System.out.println(quadKey + ": ");

			try ( InputStream is = ds.readRawSpatialCluster(quadKey);
					RecordSet rset = PBInputStreamRecordSet.from(is) ) {
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
