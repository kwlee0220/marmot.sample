package oldbldr;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.optor.JoinOptions;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1CardSales {
	private static final String CARD_SALES = "주민/카드매출/월별_시간대/2015";
	private static final String BLOCKS = "구역/지오비전_집계구pt";
	private static final String EMD = "구역/읍면동";
	private static final String RESULT = "tmp/sales_emd";
	
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
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);

		String sumExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("sale_amt_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		sumExpr = "sale_amt = " + sumExpr;
		
		Plan plan;
		DataSet info = marmot.getDataSet(EMD);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		plan = marmot.planBuilder("읍면동별 2015년도 카드매출 집계")
					.load(CARD_SALES)
					.expand("sale_amt:double", sumExpr)
					.expand("year:int", "year=std_ym.substring(0,4);")
					.project("block_cd,year,sale_amt")
					.groupBy("block_cd")
						.taggedKeyColumns("year")
						.aggregate(SUM("sale_amt").as("sale_amt"))
					.join("block_cd", BLOCKS, "block_cd", "*,param.{the_geom}",
							new JoinOptions().workerCount(64))
					.spatialJoin("the_geom", EMD, INTERSECTS,
							"*-{the_geom},param.{the_geom,emd_cd,emd_kor_nm as emd_nm}")
					.groupBy("emd_cd")
						.taggedKeyColumns(geomCol + ",year,emd_nm")
						.workerCount(1)
						.aggregate(SUM("sale_amt").as("sale_amt"))
					.project(String.format("%s,*-{%s}", geomCol, geomCol))
					.store(RESULT)
					.build();
		
		RecordSchema schema = marmot.getOutputRecordSchema(plan);
		DataSet result = marmot.createDataSet(RESULT, schema, geomCol, srid, true);
		marmot.execute(plan);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedTimeString());
	}
}
