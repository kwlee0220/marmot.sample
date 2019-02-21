#!	/bin/bash

BASE="$HOME/development/marmot/marmot.appls/marmot.sample"
PLANS="$BASE/plans/demo/policy"
OUT_DIR="tmp/10min"


mc_run_plan $PLANS/step01.st $OUT_DIR/eldely_care_facilites_bufferred -geom_col 'the_geom(EPSG:5186)' -f
mc_cluster_dataset tmp/10min/eldely_care_facilites_bufferred

mc_run_plan $PLANS/step02.st $OUT_DIR/high_density_center -geom_col 'the_geom(EPSG:5186)' -f
mc_cluster_dataset tmp/10min/high_density_center

mc_run_plan $PLANS/step03.st $OUT_DIR/high_density_hdong -geom_col 'the_geom(EPSG:5186)' -f
mc_cluster_dataset tmp/10min/high_density_hdong

mc_run_plan $PLANS/step04.st tmp/10min/elderly_care_candidates -geom_col 'the_geom(EPSG:5186)' -f
mc_cluster_dataset tmp/10min/elderly_care_candidates
