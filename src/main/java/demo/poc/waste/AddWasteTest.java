package demo.poc.waste;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.google.common.collect.Lists;

import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.JoinOptions;
import marmot.optor.StoreAsCsvOptions;
import marmot.process.NormalizeParameters;
import marmot.remote.protobuf.PBMarmotClient;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AddWasteTest {
	private static final String SGG = "연세대/사업단실증/DecisionTree/시군구";
	private static final String VAR = "연세대/사업단실증/DecisionTree/변수_면적계산전";
	private static final String TARGET = "연세대/사업단실증/DecisionTree/타겟변수_폐기물";
	
	private static final String ANALYSIS = "폐기물분석";
	private static final String ANALY_RAW_DATA = ANALYSIS + "/원시자료준비";
	private static final String ANALY_NORMALIZE = ANALYSIS + "/정규화";
	private static final String ANALY_DATA = ANALYSIS + "/학습데이터_준비";

	private static final String CSV_DT_INPUT_PATH = "tmp/decision_tree/decision_tree_input.txt";
	private static final String CSV_RESULT_PATH = "tmp/decision_tree/decision_tree_output";

	private static final String SPARK_PATH = "/usr/bin/spark-submit";
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		String[] orgVarCols = new String[] {
			"총전입_year","총전출_year",
			"순이동_year","사망건수_year","이혼건수_year","자연증가수_year","출생건수_year","혼인건수_year",
			"세대수_year","남자인구수_year","여자인구수_year","총인구수_year","세대당인구_year","건설업사업체수",
			"교육서비스업사업체수","금융및보험업사업체수","기타개인서비스업사업체수","도매및소매업사업체수",
			"보건및사회복지사업사업체수","부동산및임대업사업체수","숙박및음식점업사업체수","제조업사업체수",
			"건설업종사자수","교육서비스업종사자수","금융및보험업종사자수","기타개인서비스업종사자수",
			"도매및소매업종사자수","보건및사회복지사업종사자수","부동산및임대업종사자수","숙박및음식점업종사자수",
			"제조업종사자수","예산", "area"
		};
		
//		String[] varCols = new String[] {
//			"0_4세_인구합계_year", "5_9세_인구합계_year", "10_14세_인구합계_year","15_19세_인구합계_year",
//			"20_24세_인구합계_year", "25_29세_인구합계_year","30_34세_인구합계_year", "35_39세_인구합계_year",
//			"40_44세_인구합계_year","45_49세_인구합계_year", "50_54세_인구합계_year", "55_59세_인구합계_year",
//			"60_64세_인구합계_year", "65_69세_인구합계_year", "70_74세_인구합계_year","75_79세_인구합계_year",
//			"80_84세_인구합계_year", "85_89세_인구합계_year","90_94세_인구합계_year", "95_99세_인구합계_year",
//			"100세이상_인구합계_year","0_4세_남자합계_year","5_9세_남자합계_year","10_14세_남자합계_year",
//			"15_19세_남자합계_year","20_24세_남자합계_year","25_29세_남자합계_year","30_34세_남자합계_year",
//			"35_39세_남자합계_year","40_44세_남자합계_year","45_49세_남자합계_year","50_54세_남자합계_year",
//			"55_59세_남자합계_year","60_64세_남자합계_year","65_69세_남자합계_year","70_74세_남자합계_year",
//			"75_79세_남자합계_year","80_84세_남자합계_year","85_89세_남자합계_year","90_94세_남자합계_year",
//			"95_99세_남자합계_year","100세이상_남자합계_year","0_4세_여자합계_year","5_9세_여자합계_year",
//			"10_14세_여자합계_year","15_19세_여자합계_year","20_24세_여자합계_year","25_29세_여자합계_year",
//			"30_34세_여자합계_year","35_39세_여자합계_year","40_44세_여자합계_year","45_49세_여자합계_year",
//			"50_54세_여자합계_year","55_59세_여자합계_year","60_64세_여자합계_year","65_69세_여자합계_year",
//			"70_74세_여자합계_year","75_79세_여자합계_year","80_84세_여자합계_year","85_89세_여자합계_year",
//			"90_94세_여자합계_year","95_99세_여자합계_year","100세이상_여자합계_year","총전입_year","총전출_year",
//			"순이동_year","사망건수_year","이혼건수_year","자연증가수_year","출생건수_year","혼인건수_year",
//			"세대수_year","남자인구수_year","여자인구수_year","총인구수_year","세대당인구_year","건설업사업체수",
//			"교육서비스업사업체수","금융및보험업사업체수","기타개인서비스업사업체수","도매및소매업사업체수",
//			"보건및사회복지사업사업체수","부동산및임대업사업체수","숙박및음식점업사업체수","제조업사업체수",
//			"건설업종사자수","교육서비스업종사자수","금융및보험업종사자수","기타개인서비스업종사자수",
//			"도매및소매업종사자수","보건및사회복지사업종사자수","부동산및임대업종사자수","숙박및음식점업종사자수",
//			"제조업종사자수","예산"
//		};
		String prjExpr = FStream.of(orgVarCols)
								.zipWithIndex()
								.map(t -> String.format("%s as c%03d", t._1, t._2))
								.join(",", "sigungu_cd,", "");
		List<String> inCols = FStream.range(0, orgVarCols.length)
									.mapToObj(v -> String.format("c%03d", v))
									.toList();
		List<String> outCols = Lists.newArrayList(inCols);
		String decl = FStream.from(inCols).map(n -> n + ":double").join(",");
		
		Plan plan;
		plan = marmot.planBuilder("변수_데이터_준비")
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
		
		NormalizeParameters params = new NormalizeParameters();
		params.inputDataset(OUTPUT(ANALY_RAW_DATA));
		params.inputFeatureColumns(inCols);
		params.outputDataset(OUTPUT(ANALY_NORMALIZE));
		params.outputFeatureColumns(outCols);
		marmot.executeProcess("normalize", params.toMap());
		
		String declExpr = FStream.from(outCols)
								.map(c -> String.format("%s:string", c))
								.join(',');
		String updExpr = FStream.from(outCols)
								.zipWithIndex(1)
								.map(t -> String.format("%s = '%d:' + (float)%s;", t._1, t._2, t._1))
								.join(" ");

		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT(' ').headerFirst(false);
		plan = marmot.planBuilder("xxxx")
					.load(OUTPUT(ANALY_NORMALIZE))
					.hashJoin("sigungu_cd", TARGET, "sigungu_cd",
								"param.가정생활폐기물 as waste, *-{sigungu_cd}",
								JoinOptions.INNER_JOIN)
					.expand(declExpr, updExpr)
					.storeAsCsv(CSV_DT_INPUT_PATH, opts)
					.build();
		marmot.execute(plan);
		

//		String colDecl = FStream.from(inCols).map(name -> name + ":double").join(",");
//		AggregateFunction[] aggrs = FStream.from(inCols)
//											.flatMapArray(name -> new AggregateFunction[] {
//												MIN(name).as(name + "_min"),
//												MAX(name).as(name + "_max")
//											})
//				.toArray(AggregateFunction.class);
//		
//		plan = marmot.planBuilder("xxx")
//					.load(OUTPUT(ANALY_DATA))
//					.aggregate(aggrs)
//					.build();
//		String initExpr1 = FStream.of(marmot.executeToRecord(plan).get().getAll())
//								.buffer(2, 2)
//								.map(pair -> (double)pair.get(1) - (double)pair.get(0))
//								.map(Object::toString)
//								.join(",", "$total = [", "]");
//		String initExpr2 = FStream.of(marmot.executeToRecord(plan).get().getAll())
//									.buffer(2, 2)
//									.map(pair -> (double)pair.get(0))
//									.map(Object::toString)
//									.join(",", "$min = [", "]");
//		String initExpr = initExpr1 + "; " + initExpr2;
//		
//		String updExpr = FStream.from(outCols).zipWithIndex()
//								.map(t -> String.format("%s = (%s-$min[%d])/$total[%d];",
//													outCols.get(t._2), inCols.get(t._2), t._2, t._2))
//								.join(" ");
//		
//		plan = marmot.planBuilder("attach_portion")
//				.load(OUTPUT(ANALY_DATA))
//				.update(RecordScript.of(initExpr, updExpr))
//				.store("tmp/result", FORCE)
//				.build();
//		marmot.execute(plan);
		
		System.out.println("done");
	}
	
	private static String OUTPUT(String analId) {
		return "/tmp/" + analId;
	}
}
