package anyang.energe;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.AggregateFunction.SUM;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SquareGrid;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class A06_GridAnalysisGas {
	private static final String INPUT = "tmp/anyang/map_gas";
	private static final String OUTPUT = "tmp/anyang/grid/grid_gas";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		DataSet cadastral = marmot.getDataSet(INPUT);
		Envelope bounds = cadastral.getBounds();
		Size2d cellSize = new Size2d(1000, 1000);
		
		int[] years = {2011, 2012, 2013, 2014, 2015, 2016, 2017};
		
		String updateExpr = Arrays.stream(years)
									.mapToObj(year -> String.format("gas_%d *= ratio", year))
									.collect(Collectors.joining("; "));
		List<AggregateFunction> aggrs = Arrays.stream(years)
											.mapToObj(year -> SUM("gas_"+year).as("value_" + year))
											.collect(Collectors.toList());
		
		Plan plan;
		plan = marmot.planBuilder("가스 사용량 격자 분석")
					.load(INPUT)
					.assignSquareGridCell("the_geom", new SquareGrid(bounds, cellSize))
					.intersection("the_geom", "cell_geom", "overlap")
					.expand1("ratio:double", "(ST_Area(overlap) /  ST_Area(the_geom))")
					.update(updateExpr)
					.groupBy("cell_id")
						.tagWith("cell_geom,cell_pos")
						.workerCount(17)
						.aggregate(aggrs)
					.expand("x:long,y:long", "x = cell_pos.getX(); y = cell_pos.getY()")
					.project("cell_geom as the_geom, x, y, *-{cell_geom,x,y}")
					.store(OUTPUT)
					.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		
		for ( int year: years ) {
			extractToYear(marmot, year);
		}
		marmot.deleteDataSet(OUTPUT);
		marmot.disconnect();
		
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
	}
	
	private static void extractToYear(PBMarmotClient marmot, int year) {
		String output = OUTPUT + "_" + year;
		String projectExpr = String.format("the_geom,x,y,value_%d as value", year);
		
		DataSet ds = marmot.getDataSet(OUTPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder("연도별 격자 분석 추출")
					.load(OUTPUT)
					.project(projectExpr)
					.store(output)
					.build();
		marmot.createDataSet(output, plan, GEOMETRY(gcInfo), FORCE);
	}
	
//	private static void writeAsRaster(DataSet ds, File file, Envelope bounds, Size2d cellSize)
//		throws IllegalArgumentException, IOException {
//		GeometryColumnInfo info = ds.getGeometryColumnInfo();
//		
//		CoordinateReferenceSystem crs = CRSUtils.toCRS(info.srid());
//		Rectangle2D rect = new Rectangle2D.Double(bounds.getMinX(), bounds.getMinY(),
//												bounds.getWidth(), bounds.getHeight());
//		Envelope2D bbox = new Envelope2D(crs, rect);
//		Dimension gridDim = GeoClientUtils.divide(bounds, cellSize)
//										.ceilToInt()
//										.toDimension();
//		GridCoverage2D grid = VectorToRasterProcess.process(
//										GeoToolsUtils.toSimpleFeatureCollection(ds),
//										"value", gridDim, bbox, "value", null);
//		ArcGridWriter writer = new ArcGridWriter(new File("/home/kwlee/tmp/xxx.tiff"));
//		writer.write(grid, null);
//		writer.dispose();
//	}
}
