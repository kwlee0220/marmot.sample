#!	/bin/bash

IN="plans/anyang/minwon"
OUT="분석결과/안양대/도봉구"

mc_run_plan $IN/s01_join_parkandsent.st -output $OUT/공원_감석분석_맵 -geom_col the_geom -srid EPSG:5186 -f
mc_run_plan $IN/s02_join_parkandsent_id.st -output $OUT/공원_감석분석_맵_ID -geom_col the_geom -srid EPSG:5186 -f
mc_run_plan $IN/s03_count_parkminwon_perparcel.st -output $OUT/필지별_공원민원수 -geom_col the_geom -srid EPSG:5186 -f
mc_run_plan $IN/s04_count_parkminwon_pergrid.st -output $OUT/격자별_공원민원수 -geom_col the_geom -srid EPSG:5186 -f
mc_run_plan $IN/s05_count_parkminwon_perteamparcel.st -output $OUT/팀별_필지별_민원수 -geom_col the_geom -srid EPSG:5186 -f
mc_run_plan $IN/s06_count_parkminwon_perteamgrid.st -output $OUT/격자별_팀별_민원수 -geom_col the_geom -srid EPSG:5186 -f
