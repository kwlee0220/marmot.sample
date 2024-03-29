package bizarea;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Envelope;

import utils.Size2d;
import utils.StopWatch;

import common.SampleUtils;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordScript;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.SquareGrid;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step0 {
	private static final String LAND_USAGE = "토지/용도지역지구";
	private static final String POLITICAL = "구역/통합법정동";
	private static final String BLOCK_CENTERS = "구역/지오비전_집계구_Point";
	private static final String CADASTRAL = "구역/연속지적도_2017";
	private static final String TEMP_BIG_CITIES = "tmp/bizarea/big_cities";
	private static final String TEMP_BIZ_AREA = "tmp/bizarea/area";
	private static final String BIZ_GRID = "tmp/bizarea/grid100";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
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
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", srid);
		
		Plan plan = Plan.builder("대도시 상업지역 100mX100m 그리드 구역 생성")
						// 용도지구에 대한 100m 크기의 그리드를 생성 
						.loadGrid(new SquareGrid(bounds, cellSize), -1)
						// 상업지구에 겹치는 그리드 셀만 추출한다.
						.spatialSemiJoin("the_geom", TEMP_BIZ_AREA)
						// 상업지구 그리드 셀에 대해 대도시 영역만을 선택하고,
						// 행정도 코드(sgg_cd)를 부여한다.
						.spatialJoin("the_geom", TEMP_BIG_CITIES, "*-{cell_pos},param.sgg_cd")
						// 소지역 코드 (block_cd)를 부여한다.
						.spatialJoin("the_geom", BLOCK_CENTERS, "*-{cell_pos},param.block_cd")
						.store(BIZ_GRID, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		
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
		GeometryColumnInfo gcInfo = political.getGeometryColumnInfo();
		
		Plan plan = Plan.builder("대도시지역 추출")
								.load(POLITICAL)
								.expand("sid_cd:string,sgg_cd:string",
										"sid_cd = bjd_cd.substring(0,2);"
										+ "sgg_cd = bjd_cd.substring(0,5);")
								.filter(RecordScript.of("$sid_cd.contains(sid_cd) || $sgg_cd.contains(sgg_cd)", initExpr))
								.store(result, FORCE(gcInfo))
								.build();
		marmot.execute(plan);
		
		return marmot.getDataSet(result);
	}

	private static final DataSet filterBigCities2(MarmotRuntime marmot, String result) {
		String initExpr = String.format("$sid_cd=%s; $sgg_cd=%s", SIDO_EXPR, SGG_EXPR);

		DataSet political = marmot.getDataSet(CADASTRAL);
		GeometryColumnInfo gcInfo = political.getGeometryColumnInfo();
		
		Plan plan = Plan.builder("filter_big_cities")
								.load(CADASTRAL)
								.expand("sid_cd:string,sgg_cd:string",
										"sid_cd = pnu.substring(0,2);"
										+ "sgg_cd = pnu.substring(0,5);")
								.filter(RecordScript.of("$sid_cd.contains(sid_cd) || $sgg_cd.contains(sgg_cd)", initExpr))
								.project("the_geom,pnu")
								.store(result, FORCE(gcInfo))
								.build();
		marmot.execute(plan);
		
		return marmot.getDataSet(result);
	}

	private static final DataSet filterBizArea(MarmotRuntime marmot, String result)
		throws Exception {
		String listExpr = Arrays.asList("일반상업지역","유통상업지역","근린상업지역", "중심상업지역")
								.stream()
								.map(str -> "'" + str + "'")
								.collect(Collectors.joining(",", "[", "]"));
		String initExpr = String.format("$types = %s", listExpr);
		
		DataSet ds = marmot.getDataSet(LAND_USAGE);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		Plan plan = Plan.builder("상업지역 추출")
							.load(LAND_USAGE)
							.filter(RecordScript.of(initExpr, "$types.contains(dgm_nm)"))
							.project("the_geom")
							.store(TEMP_BIZ_AREA, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		
		return marmot.getDataSet(TEMP_BIZ_AREA);
	}
}
