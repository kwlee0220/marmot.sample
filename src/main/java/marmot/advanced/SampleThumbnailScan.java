package marmot.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.geo.query.ThumbnailScan;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleThumbnailScan {
	private static final String INPUT = "건물/건물통합정보마스터/201809";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		Envelope range = getSeoChoGu(marmot);
		
		ThumbnailScan scan = ThumbnailScan.on(input, range, 500);
		try ( RecordSet rset = scan.run() ) {
			long count = rset.count();
			System.out.println("scan count: " + count);
			if ( count > 500 ) {
				throw new AssertionError();
			}
		}
	}

	private static final String SGG = "구역/시군구";
	private static Envelope getSeoChoGu(MarmotRuntime marmot) {
		Plan plan = marmot.planBuilder("get seochogu")
							.load(SGG)
							.filter("sig_cd == 11650")
							.project("the_geom")
							.build();
		return marmot.executeToGeometry(plan).get().getEnvelopeInternal();
	}
}
