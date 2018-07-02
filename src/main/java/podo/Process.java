package podo;

import static marmot.optor.AggregateFunction.UNION;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.rset.GeoJsonRecordSetWriter;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Process {
	private static final String LAND_COVER_1987 = "토지/토지피복도/1987";
	private static final String LAND_COVER_2007 = "토지/토지피복도/2007";
	private static final String OUTPUT_1987_S = "토지/토지피복도/1987S";
	private static final String OUTPUT_2007_S = "토지/토지피복도/2007S";
	private static final String RESULT = "tmp/result";
	
	private static StopWatch s_watch;
	
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
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		s_watch = StopWatch.start();
		
		splitCovers(marmot);
		
		cluster(marmot, OUTPUT_1987_S);
		cluster(marmot, OUTPUT_2007_S);
		
		DataSet result = analysis(marmot);
		export(marmot, result, new File("result.gjson"));
		
		System.out.println("완료: 토지피복도 변화량 분석");
		System.out.printf("elapsed time=%s%n", s_watch.getElapsedMillisString());
	}
	
	private static void export(PBMarmotClient marmot, DataSet ds, File file) throws IOException {
		StopWatch watch = StopWatch.start();
		System.out.printf("결과파일 생성: %s...", file.getAbsolutePath());
		GeoJsonRecordSetWriter.into(file.getAbsolutePath()).write(ds);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static DataSet analysis(PBMarmotClient marmot) {
		StopWatch watch = StopWatch.start();
		System.out.printf("토지피복도 공간조인...");
		
		String colExpr = "left.{the_geom, cover as c1987, uid as uid1987},"
						+ "right.{the_geom as g2,cover as c2007, uid as uid2007";
		Plan plan = marmot.planBuilder("토지피복_변화량")
						.loadSpatialIndexJoin(OUTPUT_1987_S, OUTPUT_2007_S, INTERSECTS, colExpr)
						.intersection("the_geom", "g2", "the_geom")
						.groupBy("uid1987,uid2007")
							.tagWith("c1987,c2007")
							.workerCount(1)
							.aggregate(UNION("the_geom").as("the_geom"))
						.project("the_geom,c1987,c2007")
						.store(RESULT)
						.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		DataSet result = marmot.createDataSet(RESULT, gcInfo, plan, true);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		return result;
	}
	
	private static void cluster(PBMarmotClient marmot, String dsId) {
		StopWatch watch = StopWatch.start();
		System.out.printf("토지피복도 인덱싱: %s...", dsId);
		DataSet ds = marmot.getDataSet(dsId);
		ds.cluster();
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static void splitCovers(PBMarmotClient marmot) throws InterruptedException {
		CompletableFuture<Void> future
					= CompletableFuture.runAsync(()
									-> split(marmot, LAND_COVER_1987, OUTPUT_1987_S));
		Thread.sleep(120 * 1000);
		split(marmot, LAND_COVER_2007, OUTPUT_2007_S);
		
		future.join();
	}
	
	private static void split(PBMarmotClient marmot, String inputDsId, String outputDsId) {
		StopWatch watch = StopWatch.start();
		System.out.printf("토지피복도 분할: %s...", outputDsId);
		
		DataSet ds = marmot.getDataSet(inputDsId);
		String geomCol = ds.getGeometryColumn();
		
		Plan plan = marmot.planBuilder(inputDsId + "_분할")
							.load(inputDsId, 16)
							.project("the_geom, 분류구분 as cover")
							.assignUid("uid")
							.splitGeometry(geomCol)
							.drop(0)
							.store(outputDsId)
							.build();
		DataSet result = marmot.createDataSet(outputDsId, ds.getGeometryColumnInfo(), plan, true);
		
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
