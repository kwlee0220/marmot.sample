package marmot.advanced;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClient;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleQuerySpatialCluster {
	private static final String INPUT = "건물/건물통합정보마스터";
	private static final String SGG = "구역/시군구";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();
		
		Envelope bounds = getSeoChoGu(marmot);
		
		DataSet buildings = marmot.getDataSet(INPUT);
		List<String> quadKeys = buildings.querySpatialClusterKeyAll(bounds);
		System.out.println("quad_keys:  " + quadKeys);
		
		Option<String> cqlExpr = Option.some("grnd_flr >= 5");
		try ( RecordSet rset = buildings.readSpatialCluster(quadKeys.get(0), cqlExpr) ) {
			SampleUtils.printPrefix(rset, 10);
		}
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