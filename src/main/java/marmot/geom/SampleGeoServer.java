package marmot.geom;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.command.MarmotClientCommands;
import marmot.geo.geoserver.rest.GeoServer;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleGeoServer {
//	private static final String INPUT = "건물/건물통합정보마스터/201809";
//	private static final String SIDO = "구역/시도";
//	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	private static final String HOSPITAL = "POI/병원";
	private static final String CHILDREN = "POI/어린이보호구역";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		DataSet ds = marmot.getDataSet(CHILDREN);
		
		GeoServer server = GeoServer.create("localhost", 8080, "admin", "geoserver");
		
		System.out.println("before remove layer: " + server.listLayers());

		server.removeLayer(ds.getId());
		System.out.println("after remove layer: " + server.listLayers());
		server.addLayer(ds);
		System.out.println("after add layer: " + server.listLayers());
		server.removeLayer(ds.getId());
		System.out.println("after remove layer again: " + server.listLayers());
	}
}
