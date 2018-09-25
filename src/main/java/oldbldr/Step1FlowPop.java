package oldbldr;

import static marmot.optor.AggregateFunction.AVG;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1FlowPop {
	private static final String FLOW_POP = "주민/유동인구/월별_시간대/2015";
	private static final String EMD = "구역/읍면동";
	private static final String RESULT = "tmp/flowpop_emd";
	
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
		
		String handleNull = IntStream.range(0, 24)
				.mapToObj(idx -> String.format("if ( avg_%02dtmst == null ) { avg_%02dtmst = 0; }%n", idx, idx))
				.collect(Collectors.joining());

		String avgExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.collect(Collectors.joining("+", "(", ")"));
		avgExpr = String.format("avg = %s / 24;", avgExpr);
		String script = avgExpr + "year=std_ym.substring(0,4);";
		
		DataSet input = marmot.getDataSet(EMD);
		String geomCol = input.getGeometryColumn();
		
		Plan plan = marmot.planBuilder("읍면동별 2015년도 유동인구 집계")
							.load(FLOW_POP)
							.update(handleNull)
							.expand("avg:double, year:int", script)
							.project("the_geom,block_cd,year,avg")
							.spatialJoin("the_geom", EMD,
									"*-{the_geom},param.{the_geom,emd_cd,emd_kor_nm as emd_nm}")
							.groupBy("emd_cd")
								.tagWith(geomCol + ",year,emd_nm")
								.workerCount(1)
								.aggregate(AVG("avg").as("avg"))
							.project(String.format("%s,*-{%s}", geomCol, geomCol))
							.store(RESULT)
							.build();
		DataSet result = marmot.createDataSet(RESULT, input.getGeometryColumnInfo(), plan, DataSetOption.FORCE);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
