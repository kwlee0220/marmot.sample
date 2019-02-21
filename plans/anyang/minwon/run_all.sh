#!	/bin/bash

BASE="$HOME/development/marmot/marmot.appls/marmot.sample/plans/anyang/minwon"
OUT="분석결과/안양대/도봉구"

mc_run_plan $BASE/s01_join_parkandsent.st $OUT/공원_감석분석_맵 -geom_col 'the_geom(EPSG:5186)' -f
mc_run_plan $BASE/s02_join_parkandsent_id.st $OUT/공원_감석분석_맵_ID -geom_col 'the_geom(EPSG:5186)' -f
mc_run_plan $BASE/s03_count_parkminwon_perparcel.st $OUT/필지별_공원민원수 -geom_col 'the_geom(EPSG:5186)' -f
mc_run_plan $BASE/s04_count_parkminwon_pergrid.st $OUT/격자별_공원민원수 -geom_col 'the_geom(EPSG:5186)' -f
mc_run_plan $BASE/s05_count_parkminwon_perteamparcel.st $OUT/팀별_필지별_민원수 -geom_col 'the_geom(EPSG:5186)' -f
mc_run_plan $BASE/s06_count_parkminwon_perteamgrid.st $OUT/격자별_팀별_민원수 -geom_col 'the_geom(EPSG:5186)' -f
