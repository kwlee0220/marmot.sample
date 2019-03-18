#!	/bin/bash

BASE="$HOME/development/marmot/marmot.appls/marmot.sample"
PLANS="$BASE/plans/oldbld"

mc_run_plan $PLANS/step_1_blocks.st tmp/oldbld/blocks_emd -f
mc_run_plan $PLANS/step_2_buildings.st tmp/oldbld/buildings_emd -f
mc_run_plan $PLANS/step_3_card_sale.st tmp/oldbld/card_sale_emd -f
mc_run_plan $PLANS/step_4_pop.st tmp/oldbld/pop_emd -f
mc_run_plan $PLANS/step_5_result.st tmp/oldbld/result -g 'the_geom(EPSG:5186)' -f

mc_delete tmp/oldbld/blocks_emd
mc_delete tmp/oldbld/buildings_emd
mc_delete tmp/oldbld/card_sale_emd
mc_delete tmp/oldbld/pop_emd