package yunsei;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Y2T_1_2 {
	private static final String BUS_OT_DT = "연세대/서울버스_승하차";
	private static final String SID = "구역/시도";
	private static final String COLLECT = "구역/집계구";
	private static final String TEMP_BUS_SEOUL = "tmp/bus_seoul";
	private static final String MULTI_RINGS = "tmp/multi_rings";
	private static final String TEMP_JOINED = "tmp/joined";
	private static final String RESULT = "tmp/result";
	
	private static final Map<Integer,Float> RATIOS = Maps.newHashMap();
	static {
		RATIOS.put(100, 0.1f);
		RATIOS.put(200, 0.2f);
		RATIOS.put(300, 0.3f);
		RATIOS.put(400, 0.4f);
	}
	
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

		plan = marmot.planBuilder("crop")
					.load(BUS_OT_DT)
					// 서울시 영역만 추출한다.
					.intersects(geomCol, seoul)
					.store(TEMP_BUS_SEOUL)
					.build();
		result = marmot.createDataSet(TEMP_BUS_SEOUL, gcInfo, plan, DataSetOption.FORCE);
		watch.stop();
		
		DataSet buffereds = null;
		for ( int radius: Arrays.asList(100, 200, 300, 400) ) {
			StringBuilder builder = new StringBuilder();
			builder.append(String.format("area=ST_Area(%s);%n", geomCol));
			for ( int i =1; i <= 25; ++i ) {
				builder.append(String.format("ot%02d = ot%02d * %.1f;%n", i, i, RATIOS.get(radius)));
				builder.append(String.format("dt%02d = dt%02d * %.1f;%n", i, i, RATIOS.get(radius)));
			}
			String expr = builder.toString();	
			
			plan = marmot.planBuilder("spread")
						.load(TEMP_BUS_SEOUL)
						.buffer(geomCol, radius)
						.expand("area:double", expr)
						.store(MULTI_RINGS)
						.build();
			if ( buffereds == null ) {
				buffereds = marmot.createDataSet(MULTI_RINGS, gcInfo, plan, DataSetOption.FORCE);
			}
			else {
				marmot.execute(plan);
			}
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append(String.format("ratio = ST_Area(the_geom) / area;%n"));
		for ( int i =1; i <= 25; ++i ) {
			builder.append(String.format("ot%02d *= ratio; dt%02d *= ratio;%n", i, i));
		}
		String expr = builder.toString();

		List<AggregateFunction> aggrFuncList = Lists.newArrayList();
		for ( int i =1; i <= 25; ++i ) {
			String otCol = String.format("ot%02d", i);
			aggrFuncList.add(AggregateFunction.SUM(otCol).as(otCol));
			String dtCol = String.format("dt%02d", i);
			aggrFuncList.add(AggregateFunction.SUM(dtCol).as(dtCol));
		}

		List<String> valueColNames = IntStream.rangeClosed(1, 25)
											.mapToObj(idx -> (Integer)idx)
											.flatMap(idx -> {
												String ot = String.format("ot%02d", idx);
												String dt = String.format("dt%02d", idx);
												return Stream.of(ot, dt);
											})
											.collect(Collectors.toList());
		plan = marmot.planBuilder("analysis")
					.load(COLLECT)
					
//					.buildSpatialHistogram(geomCol, MULTI_RINGS, valueColNames)
					.intersectionJoin(geomCol, COLLECT, "*,param.tot_oa_cd,param.the_geom as param_geom")
					.expand("ratio:double", expr)
					.store(TEMP_JOINED)
					.build();
		marmot.createDataSet(TEMP_JOINED, gcInfo, plan, DataSetOption.FORCE);
		
		plan = marmot.planBuilder("analysis")
					.load(TEMP_JOINED)
					.groupBy("tot_oa_cd")
						.tagWith("param_geom")
						.aggregate(aggrFuncList)
					.project("param_geom as the_geom, *-{param_geom}")
					.store(RESULT)
					.build();
		marmot.createDataSet(RESULT, gcInfo, plan, DataSetOption.FORCE);
		
//		ClusterWithKMeansParameters params = new ClusterWithKMeansParameters();
//		params.dataset(INPUT);
//		params.outputDataset(OUTPUT);
//		params.featureColumns("center");
//		params.clusterColumn("group");
//		params.initialPoints(getInitCentroids(marmot, 9, 0.025));
//		params.terminationDistance(100);
//		params.terminationIterations(30);
		
		System.out.println("elapsed: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
