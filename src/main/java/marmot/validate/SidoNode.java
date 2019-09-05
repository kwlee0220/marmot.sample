package marmot.validate;

import static marmot.StoreDataSetOptions.FORCE;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordScript;
import utils.stream.IntFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SidoNode extends RootNode {
	public SidoNode(String name, String dsId, String key, int keyLen, String outputPrefix) {
		super(name, dsId, key, keyLen, outputPrefix);
	}
	
	@Override
	public void validateKey(MarmotRuntime marmot) {
		super.validateKey(marmot);
		
		findInvalidKeys(marmot);
	}

	private void findInvalidKeys(MarmotRuntime marmot) {
		String sidoListStr = IntFStream.of(11, 26, 27, 28, 29, 30, 31, 36,
											41, 42, 43, 44, 45, 46, 47, 48, 50)
										.mapToObj(idx -> String.format("'%d'", idx))
										.join(",", "[", "]");
		String filterInitExpr = "$sido_list = " + sidoListStr;
		String filterExpr = "!$sido_list.contains(CTPRVN_CD)";
		
		Plan plan;
		DataSet result;
		
		plan = marmot.planBuilder("validate sido id")
					.load(getIdDataSet())
					.filter(RecordScript.of(filterInitExpr, filterExpr))
					.store(m_prefix + "bad_keys", FORCE)
					.build();
		marmot.execute(plan);
		
		result = marmot.getDataSet(m_prefix + "bad_keys");
		System.out.printf("number of invalid keys: %d%n", result.getRecordCount());
		if ( result.getRecordCount() == 0 ) {
			marmot.deleteDataSet(result.getId());
		}
	}
}
