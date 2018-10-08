package marmot;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import io.vavr.control.Option;
import marmot.command.MarmotClient;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleQuery {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String EMD = "구역/읍면동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();
		
		Envelope bounds = getSeoChoDong(marmot);
		
		DataSet ds = marmot.getDataSet(INPUT);
		try ( RecordSet rset = ds.queryRange(bounds, Option.some("trnsit_yn = '1'")) ) {
			SampleUtils.printPrefix(rset, 5);
		}
	}
	
	private static Envelope getSeoChoDong(MarmotRuntime marmot) {
		Plan plan = marmot.planBuilder("get seochodong")
							.load(EMD)
							.filter("emd_cd==11650108")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
}
