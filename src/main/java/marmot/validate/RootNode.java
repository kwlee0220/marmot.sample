package marmot.validate;

import static marmot.optor.StoreDataSetOptions.FORCE;

import marmot.MarmotRuntime;
import marmot.Plan;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RootNode extends Node {
	public RootNode(String name, String dsId, String key, int keyLen, String outputPrefix) {
		super(name, dsId, key, keyLen, outputPrefix);
	}

	@Override
	protected void extractKeys(MarmotRuntime marmot) {
		Plan plan = Plan.builder("extract id (" + m_name + ")")
						.load(m_dsId)
						.project(m_keyCol)
						.store(getIdDataSet(), FORCE)
						.build();
		marmot.execute(plan);
	}
}
