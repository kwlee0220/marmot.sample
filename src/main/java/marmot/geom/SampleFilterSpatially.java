package marmot.geom;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;
import static marmot.optor.geo.SpatialRelation.IS_CONTAINED_BY;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.geo.query.GeoDataStore;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFilterSpatially {
	private static final String INPUT = "건물/GIS건물통합정보_2019";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		GeoDataStore store = GeoDataStore.from(marmot).setUsePrefetch(true);
		
		for ( DataSet ds: store.getGeoDataSetAll() ) {
			System.out.println(ds);
		}
		
		RecordSet rset;
		Envelope bounds;
		
		// 서초구 
		bounds = new Envelope(198216.9927942209, 208471.44085357513,
								536547.4066811937, 547344.8814057615);
		filterIntersects(marmot, bounds);
		filterIsContainedBy(marmot, bounds);
		
		// 송파구 
		bounds = new Envelope(205966.85735437565, 214274.61621185002,
								540795.2117468617, 549301.5654773772);
		filterIntersects(marmot, bounds);
		filterIsContainedBy(marmot, bounds);
		
		// 노원구 
		bounds = new Envelope(203677.14832563806, 209934.18424544204,
								557188.3157193642, 566276.2981287376);
		filterIntersects(marmot, bounds);
		filterIsContainedBy(marmot, bounds);
	}
	
	private static void filterIntersects(MarmotRuntime marmot, Envelope bounds) {
		Plan plan;
		plan = marmot.planBuilder("test")
						.load(INPUT)
						.filterSpatially("the_geom", INTERSECTS, bounds)
						.aggregate(COUNT())
						.build();
		long count = marmot.executeToLong(plan).get();
		System.out.printf("range=%s, count=%d%n", bounds, count);
	}
	
	private static void filterIsContainedBy(MarmotRuntime marmot, Envelope bounds) {
		Plan plan;
		plan = marmot.planBuilder("test")
						.load(INPUT)
						.filterSpatially("the_geom", IS_CONTAINED_BY, bounds)
						.aggregate(COUNT())
						.build();
		long count = marmot.executeToLong(plan).get();
		System.out.printf("range=%s, count=%d%n", bounds, count);
	}
}
