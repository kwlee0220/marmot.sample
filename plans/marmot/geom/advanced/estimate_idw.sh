#!	/bin/bash

mc_run_plan $MARMOT_SAMPLE_HOME/plans/marmot/geom/advanced/estimate_idw1.st tmp/points -geom_col 'the_geom(EPSG:5186)' -f
mc_cluster_dataset tmp/points
mc_run_plan $MARMOT_SAMPLE_HOME/plans/marmot/geom/advanced/estimate_idw2.st tmp/result -geom_col 'the_geom(EPSG:5186)' -f