package marmot.validate;

import static marmot.DataSetOption.FORCE;
import static marmot.optor.JoinOptions.INNER_JOIN;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LiNode extends NonRootNode {
	public LiNode(Node parent, String name, String dsId, String key, int keyLen,
				String outputPrefix) {
		super(parent, name, dsId, key, keyLen, outputPrefix);
	}
	
	@Override
	protected void findInvalidLinks(MarmotRuntime marmot) {
		super.findInvalidLinks(marmot);
		
		marmot.deleteDataSet(m_prefix + "__tmp");
		marmot.moveDataSet(m_prefix + SUFFIX_EMPTY_PARENT, m_prefix + "__tmp");
		
		try {
			String outCols = String.format("right.{%s,emd_kor_nm}", m_parent.m_keyCol);
			Plan plan = marmot.planBuilder("adjust empty childs")
							.loadHashJoin(m_prefix + "__tmp", m_parent.m_keyCol,
											m_parent.m_dsId, m_parent.m_keyCol,
											outCols, INNER_JOIN())
							.filter("emd_kor_nm.endsWith('읍')")
							.build();
			DataSet result = marmot.createDataSet(m_prefix + SUFFIX_EMPTY_PARENT, plan, FORCE);
			System.out.printf("%s: number of empty parents (revised): %d%n", m_name, result.getRecordCount());
			if ( result.getRecordCount() == 0 ) {
				marmot.deleteDataSet(result.getId());
			}
		}
		finally {
			marmot.deleteDataSet(m_prefix + "__tmp");
		}
	}
}
