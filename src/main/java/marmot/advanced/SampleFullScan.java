package marmot.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.geo.query.FullScan;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFullScan {
	private static final String INPUT = "POI/주유소_가격";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet input = marmot.getDataSet(INPUT);
		
		FullScan scan = FullScan.on(marmot, input);
		try ( RecordSet rset = scan.run() ) {
			long count = rset.count();
			System.out.println("scan count: " + count);
			if ( count != 11093 ) {
				throw new AssertionError();
			}
		}
		
		scan.setSampleCount(50);
		try ( RecordSet rset = scan.run() ) {
			double ratio = scan.getSampleRatio();
			long count = rset.count();
			
			System.out.printf("scan count: %d, ratio: %f%n", count, ratio);
			if ( count > 50 ) {
				throw new AssertionError();
			}
		}

		scan = FullScan.on(marmot, input);
		scan.setRange(getSeoChoGu(marmot));
		
		try ( RecordSet rset = scan.run() ) {
			double ratio = scan.getSampleRatio();
			long count = rset.count();
			System.out.printf("scan count: %d, ratio: %f%n", count, ratio);
		}
		
		scan.setSampleCount(50);
		try ( RecordSet rset = scan.run() ) {
			double ratio = scan.getSampleRatio();
			long count = rset.count();
			
			System.out.printf("scan count: %d, ratio: %f%n", count, ratio);
			if ( count > 50 ) {
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
