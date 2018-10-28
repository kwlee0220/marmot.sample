#!	/bin/bash

mc_run_plan plans/appls/find_best_subway_1.st -p -output 분석결과/지하철역사_추천/지하철역사_버퍼_그리드 -geom_col the_geom -srid EPSG:5186 -f
mc_run_plan plans/appls/find_best_subway_2.st -p -output tmp/seoul -geom_col the_geom -srid EPSG:5186 -f

mc_run_plan plans/appls/find_best_subway_3.st -p -output 분석결과/지하철역사_추천/역사외_지역/유동인구/격자별_집계 -geom_col the_geom -srid EPSG:5186 -f
mc_process_normalize 분석결과/지하철역사_추천/역사외_지역/유동인구/격자별_집계 분석결과/지하철역사_추천/유동인구 -in_features avg -out_features normalized -f

mc_run_plan plans/appls/find_best_subway_4.st -p -output 분석결과/지하철역사_추천/역사외_지역/택시로그/격자별_집계 -geom_col the_geom -srid EPSG:5186 -f
mc_process_normalize 분석결과/지하철역사_추천/역사외_지역/택시로그/격자별_집계 분석결과/지하철역사_추천/택시로그 -in_features count -out_features normalized -f

mc_delete tmp/seoul
mc_run_plan plans/appls/find_best_subway_5.st -p -output 분석결과/지하철역사_추천/최종결과 -geom_col the_geom -srid EPSG:5186 -f

