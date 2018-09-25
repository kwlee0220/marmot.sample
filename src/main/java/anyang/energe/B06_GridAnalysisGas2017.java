package anyang.energe;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.AggregateFunction.SUM;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.AggregateFunction;
import marmot.optor.geo.SquareGrid;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.StopWatch;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B06_GridAnalysisGas2017 {
	private static final String INPUT = "tmp/anyang/map_gas2017";
	private static final String OUTPUT = "tmp/anyang/grid/grid_gas2017";
	
	private static final List<String> COL_NAMES = FStream.rangeClosed(1, 12)
														.map(i -> "month_" + i)
														.toList();
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		DataSet cadastral = marmot.getDataSet(INPUT);
		Envelope bounds = cadastral.getBounds();
		Size2d cellSize = new Size2d(1000, 1000);
		
		String updateExpr = FStream.of(COL_NAMES)
									.map(c -> String.format("%s *= ratio", c))
									.join("; ");
		List<AggregateFunction> aggrs = FStream.of(COL_NAMES)
												.map(c -> SUM(c).as(c))
												.toList();
		
		Plan plan;
		plan = marmot.planBuilder("2017 가스 사용량 격자 분석")
					.load(INPUT)
					.assignSquareGridCell("the_geom", new SquareGrid(bounds, cellSize))
					.intersection("the_geom", "cell_geom", "overlap")
					.expand1("ratio:double", "(ST_Area(overlap) /  ST_Area(the_geom))")
					.update(updateExpr)
					.groupBy("cell_id")
						.tagWith("cell_geom,cell_pos")
						.workerCount(7)
						.aggregate(aggrs)
					.expand("x:long,y:long", "x = cell_pos.getX(); y = cell_pos.getY()")
					.project("cell_geom as the_geom, x, y, *-{cell_geom,x,y}")
					.store(OUTPUT)
					.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		marmot.createDataSet(OUTPUT, gcInfo, plan, GEOMETRY(gcInfo), FORCE);
		
		for ( int month = 1; month <= 12; ++month ) {
			extractToMonth(marmot, month);
		}
		marmot.deleteDataSet(OUTPUT);
		marmot.disconnect();
		
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
	}
	
	private static void extractToMonth(PBMarmotClient marmot, int month) {
		String output = String.format("%s_splits/%d", OUTPUT, month);
		String projectExpr = String.format("the_geom,x,y,month_%d as value", month);
		
		DataSet ds = marmot.getDataSet(OUTPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder("월별 격자 분석 추출")
					.load(OUTPUT)
					.project(projectExpr)
					.store(output)
					.build();
		marmot.createDataSet(output, plan, GEOMETRY(gcInfo), FORCE);
	}
}
