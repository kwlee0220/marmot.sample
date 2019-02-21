#!	/bin/bash

BASE="$HOME/development/marmot/marmot.appls/marmot.sample"
PLANS="$BASE/plans/bizarea"


mc_run_plan $PLANS/step_0.st tmp/bizarea/big_cities -geom_col 'the_geom(EPSG:5186)' -f
mc_run_plan $PLANS/step_1.st tmp/bizarea/area -geom_col 'the_geom(EPSG:5186)' -f
mc_run_plan $PLANS/step_2.st tmp/bizarea/grid100 -geom_col 'the_geom(EPSG:5186)' -f
mc_delete tmp/bizarea/big_cities
mc_delete tmp/bizarea/area


mc_run_plan $PLANS/step_3.st tmp/bizarea/grid100_land -geom_col 'the_geom(EPSG:5186)' -f
mc_run_plan $PLANS/step_4.st tmp/bizarea/grid100_sales -geom_col 'the_geom(EPSG:5186)' -f
mc_run_plan $PLANS/step_5.st tmp/bizarea/grid100_pop -geom_col 'the_geom(EPSG:5186)' -f
mc_delete tmp/bizarea/grid100