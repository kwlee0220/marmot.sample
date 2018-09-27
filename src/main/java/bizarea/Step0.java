package bizarea;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotCommands;
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
public class Step0 {
	private static final String LAND_USAGE = "토지/용도지역지구";
	private static final String POLITICAL = "구역/통합법정동";
	private static final String BLOCK_CENTERS = "구역/지오비전_집계구_Point";
	private static final String CADASTRAL = "구역/연속지적도";
	private static final String TEMP_BIG_CITIES = "tmp/bizarea/big_cities";
	private static final String TEMP_BIZ_AREA = "tmp/bizarea/area";
	private static final String BIZ_GRID = "tmp/bizarea/grid100";
	
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
		
		DataSet result;
		
		//  연속지적도에서 대도시 영역을 추출한다.
		result = filterBigCities(marmot, TEMP_BIG_CITIES);
		System.out.println("대도시 영역 추출 완료, elapsed=" + watch.getElapsedMillisString());
		
		// 용도지구에서 상업지역 추출
		result = filterBizArea(marmot, TEMP_BIZ_AREA);
		System.out.println("용도지구에서 상업지역 추출 완료, elapsed=" + watch.getElapsedMillisString());

		DataSet ds = marmot.getDataSet(CADASTRAL);
		String srid = ds.getGeometryColumnInfo().srid();
		Envelope bounds = ds.getBounds();
		Size2d cellSize = new Size2d(100, 100);
		
		Plan plan = marmot.planBuilder("대도시 상업지역 100mX100m 그리드 구역 생성")
							// 용도지구에 대한 100m 크기의 그리드를 생성 
							.loadSquareGridFile(new SquareGrid(bounds, cellSize), -1)
							// 상업지구에 겹치는 그리드 셀만 추출한다.
							.spatialSemiJoin("the_geom", TEMP_BIZ_AREA)
							// 상업지구 그리드 셀에 대해 대도시 영역만을 선택하고,
							// 행정도 코드(sgg_cd)를 부여한다.
							.spatialJoin("the_geom", TEMP_BIG_CITIES, "*-{cell_pos},param.sgg_cd")
							// 소지역 코드 (block_cd)를 부여한다.
							.spatialJoin("the_geom", BLOCK_CENTERS, "*-{cell_pos},param.block_cd")
							.store(BIZ_GRID)
							.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", srid);
		result = marmot.createDataSet(BIZ_GRID, plan, GEOMETRY(gcInfo), FORCE);
		
		marmot.deleteDataSet(TEMP_BIG_CITIES);
		marmot.deleteDataSet(TEMP_BIZ_AREA);
		System.out.printf("상업지구 그리드 셀 구성 완료, elapsed: %s%n",
							watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
	
	private static final String SIDO_EXPR;
	private static final String SGG_EXPR;
	static {
		SIDO_EXPR = Arrays.asList("11","26","27", "28", "29", "30", "31")
							.stream()
							.map(str -> "'" + str + "'")
							.collect(Collectors.joining(",", "[", "]"));
		SGG_EXPR = Arrays.asList("41115","41111","41117", "41113", "48125", "48123",
								"48127", "48121", "48129", "41281", "41285", "41287")
								.stream()
								.map(str -> "'" + str + "'")
								.collect(Collectors.joining(",", "[", "]"));
	}

	private static final DataSet filterBigCities(MarmotRuntime marmot, String result) {
		String initExpr = String.format("$sid_cd=%s; $sgg_cd=%s", SIDO_EXPR, SGG_EXPR);

		DataSet political = marmot.getDataSet(POLITICAL);
		Plan plan = marmot.planBuilder("대도시지역 추출")
								.load(POLITICAL)
								.expand("sid_cd:string,sgg_cd:string",
										"sid_cd = bjd_cd.substring(0,2);"
										+ "sgg_cd = bjd_cd.substring(0,5);")
								.filter(initExpr,
										"$sid_cd.contains(sid_cd) || $sgg_cd.contains(sgg_cd)")
								.store(result)
								.build();
		GeometryColumnInfo gcInfo = political.getGeometryColumnInfo();
		DataSet ds = marmot.createDataSet(result, plan, GEOMETRY(gcInfo), FORCE);
		return ds;
	}

	private static final DataSet filterBigCities2(MarmotRuntime marmot, String result) {
		String initExpr = String.format("$sid_cd=%s; $sgg_cd=%s", SIDO_EXPR, SGG_EXPR);

		DataSet political = marmot.getDataSet(CADASTRAL);
		Plan plan = marmot.planBuilder("filter_big_cities")
								.load(CADASTRAL)
								.expand("sid_cd:string,sgg_cd:string",
										"sid_cd = pnu.substring(0,2);"
										+ "sgg_cd = pnu.substring(0,5);")
								.filter(initExpr,
										"$sid_cd.contains(sid_cd) || $sgg_cd.contains(sgg_cd)")
								.project("the_geom,pnu")
								.store(result)
								.build();
		GeometryColumnInfo gcInfo = political.getGeometryColumnInfo();
		return marmot.createDataSet(result, plan, GEOMETRY(gcInfo), FORCE);
	}

	private static final DataSet filterBizArea(MarmotRuntime marmot, String result)
		throws Exception {
		String listExpr = Arrays.asList("일반상업지역","유통상업지역","근린상업지역", "중심상업지역")
								.stream()
								.map(str -> "'" + str + "'")
								.collect(Collectors.joining(",", "[", "]"));
		String initExpr = String.format("$types = %s", listExpr);
		
		DataSet ds = marmot.getDataSet(LAND_USAGE);
		Plan plan = marmot.planBuilder("상업지역 추출")
							.load(LAND_USAGE)
							.filter(initExpr, "$types.contains(dgm_nm)")
							.project("the_geom")
							.store(TEMP_BIZ_AREA)
							.build();
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		return marmot.createDataSet(result, plan, GEOMETRY(gcInfo), FORCE);
	}
}
