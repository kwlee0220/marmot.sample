package demo.dtg;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import org.locationtech.jts.geom.Envelope;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.geo.CoordinateTransform;
import marmot.plan.PredicateOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CountInvalidGeoms {
	private static final String POLITICAL = "구역/시도";
	private static final String DTG = "교통/dtg";
	private static final String RESULT = "tmp/dtg/taggeds";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Envelope bounds = getValidWgsBounds(marmot);

		Plan plan;
		plan = Plan.builder("count invalid geometry records")
					.load(DTG)
					.toPoint("x좌표", "y좌표", "the_geom")
					.filterSpatially("the_geom", INTERSECTS, bounds, PredicateOptions.NEGATED)
					.aggregate(COUNT())
					.store(RESULT)
					.build();
		long count = marmot.executeToLong(plan).get();
		watch.stop();
		
		System.out.printf("count=%d, total elapsed time=%s%n",
							count, watch.getElapsedMillisString());
	}
	
	private static Envelope getValidWgsBounds(PBMarmotClient marmot) {
		Envelope bounds = marmot.getDataSet(POLITICAL).getBounds();
		bounds.expandBy(1);
		
		CoordinateTransform trans = CoordinateTransform.get("EPSG:5186", "EPSG:4326");
		return trans.transform(bounds);
	}
}
