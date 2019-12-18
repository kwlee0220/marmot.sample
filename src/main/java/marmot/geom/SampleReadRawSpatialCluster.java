package marmot.geom;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleReadRawSpatialCluster {
//	private static final String INPUT = "주소/건물POI";
	private static final String INPUT = "건물/건물통합정보마스터/201809";
	private static final String SIDO = "구역/시도";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Envelope range;
		DataSet ds = marmot.getDataSet(INPUT);
		
		try ( RecordSet rset = ds.readSpatialCluster("13211032233") ) {
			rset.fstream().count();
		}
		try ( RecordSet rset = ds.readSpatialCluster("13211210100") ) {
			rset.fstream().count();
		}
		
//		rangfg
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
