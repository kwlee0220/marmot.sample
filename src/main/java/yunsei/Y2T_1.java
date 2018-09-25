package yunsei;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.vividsolutions.jts.geom.Geometry;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.process.geo.FeatureVector;
import marmot.process.geo.FeatureVectorHandle;
import marmot.process.geo.KMeansParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Y2T_1 {
	private static final String BUS_OT_DT = "연세대/서울버스_승하차";
	private static final String SID = "구역/시도";
	private static final String COLLECT = "구역/집계구";
	private static final String TEMP_BUS_SEOUL = "tmp/bus_seoul";
	private static final String MULTI_RINGS = "tmp/multi_rings";
	private static final String TEMP_JOINED = "tmp/joined";
	private static final String TEMP_HISTOGRAM = "tmp/histogram";
	private static final String RESULT = "tmp/result";
	
	private static final List<String> FEATURE_COLNAMES;
	
	private static final Map<Integer,Float> RATIOS = Maps.newHashMap();
	static {
		RATIOS.put(100, 0.4f);
		RATIOS.put(200, 0.3f);
		RATIOS.put(300, 0.2f);
		RATIOS.put(400, 0.1f);
		
		Stream<String> otStrm = IntStream.rangeClosed(1, 25)
										.mapToObj(idx -> String.format("ot%02d", idx));
		Stream<String> dtStrm = IntStream.rangeClosed(1, 25)
										.mapToObj(idx -> String.format("dt%02d", idx));
		FEATURE_COLNAMES = Streams.concat(otStrm, dtStrm)
									.collect(Collectors.toList());
	}
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
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
		
		Plan plan;
		DataSet result;
		
		DataSet input = marmot.getDataSet(SID);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		String geomCol = input.getGeometryColumn();
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		plan = marmot.planBuilder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.build();
		Geometry seoul = marmot.executeLocally(plan).toList().get(0).getGeometry(geomCol);

		// 버스 승하차 정보에서 서울 구역부분만 추출한다.
		plan = marmot.planBuilder("버스 승하차에서 서울부분 추출")
					.load(BUS_OT_DT)
					// 서울시 영역만 추출한다.
					.intersects(geomCol, seoul)
					.store(TEMP_BUS_SEOUL)
					.build();

		result = marmot.createDataSet(TEMP_BUS_SEOUL, gcInfo, plan, DataSetOption.FORCE);
		System.out.println("done: crop bus_ot_dt with seoul");
		
		DataSet multiRings = doMultiRing(marmot, result, seoul, MULTI_RINGS);
		System.out.println("done: ring_buffer, elapsed=" + watch.getElapsedMillisString());
//		multiRings.cluster();
		System.out.println("done: cluster ring_buffer, elapsed=" + watch.getElapsedMillisString());
		marmot.deleteDataSet(TEMP_BUS_SEOUL);

		List<String> valueColNames = IntStream.rangeClosed(1, 25)
											.mapToObj(idx -> (Integer)idx)
											.flatMap(idx -> {
												String ot = String.format("ot%02d", idx);
												String dt = String.format("dt%02d", idx);
												return Stream.of(ot, dt);
											})
											.collect(Collectors.toList());
		plan = marmot.planBuilder("승하차 히스트그램 생성")
					.load(COLLECT)
					// 서울시 영역만 추출한다.
					.filter("행정코드.startsWith('11')")
//					.buildSpatialHistogram(geomCol, MULTI_RINGS, valueColNames)
					.store(TEMP_HISTOGRAM)
					.build();

		result = marmot.createDataSet(TEMP_HISTOGRAM, gcInfo, plan, DataSetOption.FORCE);
		
		marmot.deleteDataSet(MULTI_RINGS);
		System.out.println("done: build_histogram, elapsed=" + watch.getElapsedMillisString());
		
		kmeans(marmot, TEMP_HISTOGRAM, RESULT);
		marmot.deleteDataSet(TEMP_HISTOGRAM);
		System.out.println("done: k-means clustering");
		
		System.out.println("elapsed: " + watch.stopAndGetElpasedTimeString());
		
//		SampleUtils.printPrefix(result, 5);
	}
	
	private static void kmeans(MarmotRuntime marmot, String input, String output) {
		FeatureVectorHandle handle = new FeatureVectorHandle(FEATURE_COLNAMES);
		List<FeatureVector> centroids = handle.sampleInitialCentroids(marmot,
															TEMP_HISTOGRAM, 0.001, 6);
		
		KMeansParameters params = new KMeansParameters();
		params.inputDataset(input);
		params.outputDataset(output);
		params.featureColumns(FEATURE_COLNAMES);
		params.clusterColumn("cluster_id");
		params.initialCentroids(centroids);
		params.terminationDistance(50);
		params.terminationIteration(30);
		
		marmot.deleteDataSet(output);
		marmot.executeProcess("kmeans", params.toMap());
	}
	
	private static DataSet doMultiRing(MarmotRuntime marmot, DataSet bus, Geometry range,
										String outputDs) {
		final String geomCol = bus.getGeometryColumn();
		
		StringBuilder builder;
		DataSet multiRings = null;
		for ( int radius: Arrays.asList(100, 200, 300, 400) ) {
			builder = new StringBuilder();
			builder.append(String.format("area = ST_Area(%s);%n", geomCol));
			double ratio = RATIOS.get(radius);
			for ( int i =1; i <= 25; ++i ) {
				builder.append(String.format("ot%02d *= %.1f; dt%02d *= %.1f;%n",
											i, ratio, i, ratio));
			}
			String expr1 = builder.toString();
			
			builder = new StringBuilder();
			builder.append(String.format("ratio = ST_Area(%s)/area;%n", geomCol));
			for ( int i =1; i <= 25; ++i ) {
				builder.append(String.format("ot%02d *= ratio; dt%02d *= ratio;%n", i, i));
			}
			String expr2 = builder.toString();	
			
			StopWatch watch = StopWatch.start();
			Plan plan = marmot.planBuilder("버스_승하차수_링버퍼_배분_반경_" + radius)
							.load(bus.getId())
							.buffer(geomCol, radius)
							.expand("area:double", expr1)
							// 버퍼링 영역 중에서 서울 영역만을 추출한다
							.intersection(geomCol, range)
							.expand("ratio:double", expr2)
							.project("*-{area,ratio}")
							.store(outputDs)
							.build();
			if ( multiRings == null ) {
				multiRings = marmot.createDataSet(outputDs, bus.getGeometryColumnInfo(), plan, DataSetOption.FORCE);
			}
			else {
				marmot.execute(plan);
			}
			
			System.out.printf("done: buffer (ratius=%dm, elapsed=%s)%n",
								radius, watch.stopAndGetElpasedTimeString());
		}
		
		return multiRings;
	}
}
