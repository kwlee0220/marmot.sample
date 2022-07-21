package demo.poc.waste;

import java.util.List;

import com.google.common.collect.Lists;

import utils.StopWatch;
import utils.stream.FStream;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.analysis.module.NormalizeParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.exec.ExternAnalysis;
import marmot.optor.JoinOptions;
import marmot.optor.StoreAsCsvOptions;
import marmot.optor.StoreDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildWasteDT {
	private static final String SGG = "연세대/사업단실증/DecisionTree/시군구";
	private static final String VAR = "연세대/사업단실증/DecisionTree/변수_면적계산전";
	private static final String TARGET = "연세대/사업단실증/DecisionTree/타겟변수_폐기물";
	
	private static final String ANALYSIS = "폐기물분석";
	private static final String ANALY_RAW_DATA = ANALYSIS + "/변수_데이터_준비";
	private static final String ANALY_NORMALIZE = ANALYSIS + "/정규화";
	private static final String ANALY_DT = ANALYSIS + "/결정트리_생성";

	private static final String CSV_DT_INPUT_PATH = "tmp/decision_tree/decision_tree_input.txt";
	private static final String CSV_RESULT_PATH = "tmp/decision_tree/decision_tree_output";

	private static final String SPARK_PATH = "/usr/bin/spark-submit";
	
	private static String[] VAR_COLS = new String[] {
		"인구_00_04세","인구_05_09세","인구_10_14세","인구_15_19세","인구_20_24세","인구_25_29세",
		"인구_30_34세","인구_35_39세","인구_40_44세","인구_45_49세","인구_50_54세","인구_55_59세",
		"인구_60_64세","인구_65_69세","인구_70_74세","인구_75_79세","인구_80_84세","인구_85_89세",
		"인구_90_94세","인구_95_99세","인구_100세이상","남자_00_04세","남자_05_09세","남자_10_14세",
		"남자_15_19세","남자_20_24세","남자_25_29세","남자_30_34세","남자_35_39세","남자_40_44세",
		"남자_45_49세","남자_50_54세","남자_55_59세","남자_60_64세","남자_65_69세","남자_70_74세",
		"남자_75_79세","남자_80_84세","남자_85_89세","남자_90_94세","남자_95_99세","남자_100세이상",
		"여자_00_04세","여자_05_09세","여자_10_14세","여자_15_19세","여자_20_24세","여자_25_29세",
		"여자_30_34세","여자_35_39세","여자_40_44세","여자_45_49세","여자_50_54세","여자_55_59세",
		"여자_60_64세","여자_65_69세","여자_70_74세","여자_75_79세","여자_80_84세","여자_85_89세",
		"여자_90_94세","여자_95_99세","여자_100세이상","총전입","총전출","순이동","사망건수","이혼건수",
		"자연증가수","출생건수","혼인건수","세대수","남자인구수","여자인구수","총인구수","세대당인구",
		"건설업사업체수","교육서비스업사업체수","금융및보험업사업체수","기타개인서비스업사업체수",
		"도매및소매업사업체수","보건및사회복지사업사업체수","부동산및임대업사업체수","숙박및음식점업사업체수",
		"제조업사업체수","건설업종사자수","교육서비스업종사자수","금융및보험업종사자수","기타개인서비스업종사자수",
		"도매및소매업종사자수","보건및사회복지사업종사자수","부동산및임대업종사자수","숙박및음식점업종사자수",
		"제조업종사자수","예산","area"
	};
	
	public static final void main(String... args) throws Exception {
		StopWatch watch = StopWatch.start();
		System.out.println("시작: 시군구별 생활 폐기물 패출양 요인 분석 (DecisionTree)...... ");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		prepareTrainData(marmot, CSV_DT_INPUT_PATH);
		buildDecisionTree(marmot, CSV_RESULT_PATH);
		
		System.out.printf("output=%s, 소요시간=%s%n", CSV_RESULT_PATH, watch.getElapsedMillisString());
	}
	
	private static void prepareTrainData(MarmotRuntime marmot, String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("\t단계: 시군구별 종속 데이터 준비 -> "); System.out.flush();
		
		String prjExpr = FStream.of(VAR_COLS)
								.zipWithIndex()
								.map(t -> String.format("%s as c%03d", t._1, t._2))
								.join(",", "sigungu_cd,", "");
		List<String> inCols = FStream.range(0, VAR_COLS.length)
									.mapToObj(v -> String.format("c%03d", v))
									.toList();
		List<String> outCols = Lists.newArrayList(inCols);
		String decl = FStream.from(inCols).map(n -> n + ":double").join(",");
		
		Plan plan;
		plan = Plan.builder("변수_데이터_준비")
						.load(VAR)
						.hashJoin("sigungu_cd", SGG, "sigungu_cd",
									"*,param.{the_geom}",
									JoinOptions.INNER_JOIN)
						.defineColumn("area:double", "ST_Area(the_geom)")
						.project(prjExpr)
						.expand(decl)
						.project("*-{the_geom}")
						.store(OUTPUT(ANALY_RAW_DATA), StoreDataSetOptions.FORCE)
						.build();
		marmot.execute(plan);

		DataSet ds;
		ds = marmot.getDataSet(OUTPUT(ANALY_RAW_DATA));
		System.out.printf("%s(%d건), 소요시간=%s%n",
							ds.getId(), ds.getRecordCount(), watch.getElapsedMillisString());
		
		// ******************************************************************************
		
		watch = StopWatch.start();
		System.out.print("\t단계: 시군구별 종속 데이터 정규화 -> "); System.out.flush();
		
		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(OUTPUT(ANALY_RAW_DATA));
		params.inputFeatureColumns(inCols);
		params.outputDataset(OUTPUT(ANALY_NORMALIZE));
		params.outputFeatureColumns(outCols);
		marmot.executeProcess("normalize", params.toMap());

		ds = marmot.getDataSet(OUTPUT(ANALY_NORMALIZE));
		System.out.printf("%s(%d건), 소요시간=%s%n", ds.getId(), ds.getRecordCount(),
														watch.getElapsedMillisString());
		
		// ******************************************************************************
		
		watch = StopWatch.start();
		System.out.print("\t단계: 타겟 데이터 (시군구별 폐기물 배출양) 병합 -> "); System.out.flush();
		
		String declExpr = FStream.from(outCols)
								.map(c -> String.format("%s:string", c))
								.join(',');
		String updExpr = FStream.from(outCols)
								.zipWithIndex(1)
								.map(t -> String.format("%s = '%d:' + (float)%s;", t._1, t._2, t._1))
								.join(" ");

		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT(' ').headerFirst(false);
		plan = Plan.builder("폐기물_데이터_병합")
					.load(OUTPUT(ANALY_NORMALIZE))
					.hashJoin("sigungu_cd", TARGET, "sigungu_cd",
								"param.가정생활폐기물 as waste, *-{sigungu_cd}",
								JoinOptions.INNER_JOIN)
					.expand(declExpr, updExpr)
					.storeAsCsv(outCsvPath, opts)
					.build();
		marmot.execute(plan);
		
		System.out.printf("output=%s, 소요시간=%s%n", outCsvPath, watch.getElapsedMillisString());
	}
	
	private static void buildDecisionTree(MarmotRuntime marmot, String outCsvPath) {
		StopWatch watch = StopWatch.start();
		System.out.print("\t단계: 결정트리 학습 -> "); System.out.flush();
		
		String[] args = new String[] {
			"--class", "main.scala.DT", "extensions/dt_2.11-yarn_3.2.jar",
			CSV_DT_INPUT_PATH, CSV_RESULT_PATH, "0.7", "variance", "5", "32", "5"
		};

		ExternAnalysis anal = new ExternAnalysis(ANALY_DT, SPARK_PATH, args);
		marmot.executeAnalysis(anal);
	}
	
	private static String OUTPUT(String analId) {
		return "/tmp/" + analId;
	}
}
