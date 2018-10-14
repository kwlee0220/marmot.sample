package marmot.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.geoserver.plugin.GSPDataStore;
import marmot.geoserver.plugin.GSPFeatureSource;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleQuerySpatialCluster {
//	private static final String INPUT = "건물/건물통합정보마스터";
	private static final String INPUT = "POI/주유소_가격";
	private static final String SGG = "구역/시군구";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Envelope bounds = getSeoChoGu(marmot);
		
		GSPDataStore store = GSPDataStore.from(marmot, 10000);
		GSPFeatureSource src = (GSPFeatureSource)store.getFeatureSource("POI.주유소_가격");
		RecordSet rset = src.query(bounds);
		SampleUtils.printPrefix(rset, 10);
	}
	
	private static Envelope getSeoChoGu(MarmotRuntime marmot) {
		Plan plan = marmot.planBuilder("get seochogu")
							.load(SGG)
							.filter("sig_cd==11650")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
}
