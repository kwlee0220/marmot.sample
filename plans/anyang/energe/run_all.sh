#!	/bin/bash

BASE="$HOME/development/marmot/marmot.appls/marmot.sample"
PLANS="$BASE/plans/anyang/energe"
OUT_DIR="$HOME/tmp/anyang"

#mc_run_plan $PLANS/run_00.st $OUT_DIR/cadastral -geom_col 'the_geom(EPSG:5186)' -f
#mc_run_plan $PLANS/run_01.st $OUT_DIR/gas2017 -f
mc_run_plan $PLANS/run_02.st $OUT_DIR/electro2017 -f
