package marmot.validate;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.dataset.DataSet;
import marmot.plan.Group;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class Node {
	protected final String m_name;
	protected final String m_dsId;
	protected final String m_keyCol;
	protected final int m_keyLength;
	protected final String m_prefix;

	abstract protected void extractKeys(MarmotRuntime marmot);
	
	public Node(String name, String dsId, String key, int keyLen, String outputPrefix) {
		m_name = name;
		m_dsId = dsId;
		m_keyCol = key;
		m_keyLength = keyLen;
		m_prefix = outputPrefix + name + "/";
	}
	
	public String getIdDataSet() {
		return m_prefix + "ids";
	}
	
	public void validate(MarmotRuntime marmot) {
		validateKey(marmot);
		validateGeometry(marmot);
	}
	
	public void validateKey(MarmotRuntime marmot) {
		extractKeys(marmot);
		findInvalidLengthKeys(marmot);
		findDuplicatedKeys(marmot);
	}
	
	public void validateGeometry(MarmotRuntime marmot) {
		findOverlapedGeoms(marmot);
	}
	
	private void findInvalidLengthKeys(MarmotRuntime marmot) {
		String filterExpr = String.format("%s.length() != %d", m_keyCol, m_keyLength);
		String outDsId = m_prefix + "bad_length_keys";
		
		Plan plan = Plan.builder("find bad length keys")
							.load(getIdDataSet())
							.filter(filterExpr)
							.project(m_keyCol)
							.store(outDsId, FORCE)
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(outDsId);
		System.out.printf("%s: number of bad keys: %d%n", m_name, result.getRecordCount());
		if ( result.getRecordCount() == 0 ) {
			marmot.deleteDataSet(result.getId());
		}
	}

	private void findDuplicatedKeys(MarmotRuntime marmot) {
		String outDsId = m_prefix + "duplicated_ids";
		
		Plan plan = Plan.builder("find duplicated keys")
							.load(getIdDataSet())
							.aggregateByGroup(Group.ofKeys(m_keyCol), COUNT())
							.filter("count > 1")
							.store(outDsId, FORCE)
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(outDsId);
		System.out.printf("%s: number of duplicated keys: %d%n", m_name, result.getRecordCount());
		if ( result.getRecordCount() == 0 ) {
			marmot.deleteDataSet(result.getId());
		}
	}

	private void findOverlapedGeoms(MarmotRuntime marmot) {
		String geomCol = marmot.getDataSet(m_dsId).getGeometryColumn();
		String outColExpr = String.format("left.%s as the_geom1, left.%s as key1, "
										+ "right.%s as the_geom2, right.%s as key2",
											geomCol, m_keyCol, geomCol, m_keyCol);
		String outDsId = m_prefix + "overlaped_geoms";
		
		Plan plan = Plan.builder("find overlapping geoms")
							.loadSpatialIndexJoin(m_dsId, m_dsId, outColExpr)
							.filter("key1 < key2")
							.defineColumn("overlap:double", "ST_Area(ST_Intersection(the_geom1, the_geom2))")
							.filter("overlap > 1")
							.defineColumn("ratio1:double", "Round(overlap/ST_Area(the_geom1), 3)")
							.defineColumn("ratio2:double", "Round(overlap/ST_Area(the_geom2), 3)")
							.store(outDsId, FORCE)
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(outDsId);
		System.out.printf("%s: number of overlapping geom pairs: %d%n", m_name, result.getRecordCount());
	}
}
