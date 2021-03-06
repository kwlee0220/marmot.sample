package marmot.validate;

import static marmot.optor.JoinOptions.FULL_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.dataset.DataSet;
import marmot.optor.JoinOptions;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NonRootNode extends Node {
	static final String SUFFIX_EMPTY_PARENT = "no_child_parents";
	
	protected final Node m_parent;
	private final FOption<Integer> m_nworkers;
	
	protected String getParentKeyExpr() {
		return String.format("%s.substring(0,%d)", m_keyCol, m_parent.m_keyLength);
	}

	public NonRootNode(Node parent, String name, String dsId, String key, int keyLen,
						String outputPrefix, int nworkers) {
		super(name, dsId, key, keyLen, outputPrefix);
		
		m_parent = parent;
		m_nworkers = FOption.of(nworkers);
	}

	public NonRootNode(Node parent, String name, String dsId, String key, int keyLen,
						String outputPrefix) {
		super(name, dsId, key, keyLen, outputPrefix);
		
		m_parent = parent;
		m_nworkers = FOption.empty();
	}

	@Override
	public void validateKey(MarmotRuntime marmot) {
		super.validateKey(marmot);
		
		findInvalidLinks(marmot);
	}
	
	@Override
	public void validateGeometry(MarmotRuntime marmot) {
		super.validateGeometry(marmot);
		
		findUncoveredGeoms(marmot, m_nworkers);
	}

	@Override
	protected void extractKeys(MarmotRuntime marmot) {
		String projExpr = String.format("%s,parent_key", m_keyCol);
		
		Plan plan = Plan.builder("extract links")
							.load(m_dsId)
							.defineColumn("parent_key:string", getParentKeyExpr())
							.project(projExpr)
							.store(getIdDataSet(), FORCE)
							.build();
		marmot.execute(plan);
	}
	
	protected void findInvalidLinks(MarmotRuntime marmot) {
		DataSet temp = findUnmatchedPairs(marmot);
		try {
			if ( temp.getRecordCount() == 0 ) {
				System.out.println(m_name + ": number of orphan nodes: 0");
				System.out.println(m_name + ": number of no-child parent nodes: 0");
				
				return;
			}
			
			Plan plan;
			DataSet result;
	
			String output = m_prefix + "orphans";
			String danglingExpr = String.format("%s == null", m_parent.m_keyCol);
			plan = Plan.builder("find orphan ids")
							.load(temp.getId())
							.filter(danglingExpr)
							.project(m_keyCol)
							.store(output, FORCE)
							.build();
			marmot.execute(plan);
			
			result = marmot.getDataSet(output);
			System.out.printf("%s: number of orphans: %d%n", m_name, result.getRecordCount());
			if ( result.getRecordCount() == 0 ) {
				marmot.deleteDataSet(result.getId());
			}
	
			plan = Plan.builder("find infertile nodes")
							.load(temp.getId())
							.filter("parent_key == null")
							.project(m_parent.m_keyCol)
							.store(m_parent.m_keyCol, FORCE)
							.build();
			marmot.execute(plan);
			
			result = marmot.getDataSet(m_prefix + SUFFIX_EMPTY_PARENT);
			System.out.printf("%s: number of no-child parents (%s): %d%n",
								m_name, m_parent.m_name, result.getRecordCount());
			if ( result.getRecordCount() == 0 ) {
				marmot.deleteDataSet(result.getId());
			}
		}
		finally {
			marmot.deleteDataSet(temp.getId());
		}
	}
	
	private DataSet findUnmatchedPairs(MarmotRuntime marmot) {
		String outCols = String.format("left.{%s,parent_key}, right.%s", m_keyCol, m_parent.m_keyCol);
		String filterExpr = String.format("parent_key == null || %s == null", m_parent.m_keyCol);
		String tempDsId = m_prefix + "id_pairs";
		
		Plan plan = Plan.builder("build id pairs")
					.loadHashJoin(getIdDataSet(), "parent_key",
									m_parent.getIdDataSet(), m_parent.m_keyCol,
									outCols, FULL_OUTER_JOIN)
					.filter(filterExpr)
					.store(tempDsId, FORCE)
					.build();
		marmot.execute(plan);
		
		return marmot.getDataSet(tempDsId);
	}
	
	private void findUncoveredGeoms(MarmotRuntime marmot, FOption<Integer> nworkers) {
		String tempId = m_prefix + "tmp_bindings";
		
		createParentBindings(marmot, tempId);
		try {
			Plan plan;
			
			String output = m_prefix + "uncovered_geoms";
			plan = Plan.builder("find uncovered geoms")
						.loadHashJoin(tempId, "parent_key", m_parent.m_dsId, m_parent.m_keyCol,
										"left.*,right.the_geom as parent_geom",
										JoinOptions.INNER_JOIN(nworkers))
						.filter("!ST_Contains(parent_geom, the_geom)")
						.defineColumn("uncover:multi_polygon",
										"ST_Difference(the_geom, parent_geom)")
						.defineColumn("uncover_area:double", "Round(ST_Area(uncover), 3)")
						.filter("uncover_area > 1")
						.defineColumn("ratio:double", "Round(uncover_area/ST_Area(the_geom),3)")
						.project("the_geom," + m_keyCol + ",uncover,uncover_area,ratio")
//						.sort("ratio:DESC")
						.store(output, FORCE)
						.build();
			marmot.execute(plan);
			
			DataSet result = marmot.getDataSet(output);
			System.out.printf("%s: number of un-covered geoms: %d%n", m_name, result.getRecordCount());
			if ( result.getRecordCount() == 0 ) {
				marmot.deleteDataSet(result.getId());
			}
		}
		finally {
			marmot.deleteDataSet(tempId);
		}
	}

	private DataSet createParentBindings(MarmotRuntime marmot, String outDsId) {
		String joinExpr = String.format("the_geom, %s, parent_key", m_keyCol);
		Plan plan = Plan.builder("create a parent bindings")
							.load(m_dsId)
							.defineColumn("parent_key:string", getParentKeyExpr())
							.project(joinExpr)
							.store(outDsId, FORCE)
							.build();
		marmot.execute(plan);
		
		return marmot.getDataSet(outDsId);
	}
}
