package carloc;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcHeatMap {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String SEOUL = "시연/서울특별시";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet border = marmot.getDataSet(SEOUL);
		String srid = border.getSRID();
		Envelope envl = border.getBounds();
		Polygon key = GeoClientUtils.toPolygon(envl);
		
		DimensionDouble cellSize = new DimensionDouble(envl.getWidth() / 50,
														envl.getHeight() / 50);
		
		Plan plan = marmot.planBuilder("calc_heat_map")
							.loadSquareGridFile(envl, cellSize, 32)
							.aggregateJoin("the_geom", TAXI_LOG, INTERSECTS, COUNT())
							.store(RESULT)
							.build();
		
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		DataSet result = marmot.createDataSet(RESULT, schema, "the_geom", srid, true);
		marmot.execute(plan);
		
		SampleUtils.printPrefix(result, 5);
	}
}
