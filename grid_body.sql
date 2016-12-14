--------------------------------------------------------
--  File created - среда-Декабрь-14-2016   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package Body GRID
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY "LAB"."GRID" AS
  PROCEDURE GRID_INITIAL_ROW_SHORT AS 
    nTable        VARCHAR2(100);
    sql_text      VARCHAR2(1000);
  BEGIN
    select value_string into nTable from grid_params where name='grid_table_name';
    
    sql_text :='truncate table ' || nTable;
    execute immediate sql_text;
    
    sql_text :='insert into ' || nTable || ' select * from ' || nTable || '_BK';
    execute immediate sql_text;
    commit;
  END GRID_INITIAL_ROW_SHORT;
  
  PROCEDURE GRID_INITIAL_ROW AS 
    if_exists     NUMBER;
    row_bottom    NUMBER;
    col_left      NUMBER;
    sq_num_lat    NUMBER;
    sq_num_lon    NUMBER;
    lat1          NUMBER;
    lat2          NUMBER;
    lat2_tmp      NUMBER;
    lon1          NUMBER;
    lon2          NUMBER;
    lon2_tmp      NUMBER;
    count_cell_id NUMBER;
    count_site_id NUMBER;
    step          NUMBER;
    nTable        VARCHAR2(100);
    bsTable       VARCHAR2(100);
    sql_text      VARCHAR2(1000);
    accuracy      NUMBER;
  BEGIN
    select value into accuracy from grid_params where name='accuracy';
    select value_string into nTable from grid_params where name='grid_table_name';
    sql_text := 'select count(*) from user_tables where table_name=''' || nTable || '''';
    execute immediate sql_text into if_exists;
    IF if_exists > 0 THEN
      sql_text := 'DROP TABLE ' || nTable;
      execute immediate sql_text;
    END IF;
    sql_text := q'[
    CREATE TABLE "]' || nTable || q'[" 
      (	
        "LAT1" NUMBER, 
        "LAT2" NUMBER, 
        "LON1" NUMBER, 
        "LON2" NUMBER, 
        "LABEL" VARCHAR2(20 BYTE), 
        "COUNT_CELL_ID" NUMBER, 
        "COUNT_SITE_ID" NUMBER, 
        "SQ_NUM_LAT" NUMBER, 
        "SQ_NUM_LON" NUMBER, 
        "ROW_BOTTOM" NUMBER, 
        "COL_LEFT" NUMBER,
        "CLUSTER_LEVEL" NUMBER,
        "DONE" VARCHAR2(1 BYTE) 
       )]';
    execute immediate sql_text;
       
    select value_string into bsTable from grid_params where name='bts_table_name';
    sql_text := 'select round(min(latitude),' || accuracy || ')  from ' || bsTable || ' where site_id <> ''6001''';
    execute immediate sql_text into lat1; 
    sql_text := 'select round(min(longitude),' || accuracy || ') from ' || bsTable || ' where site_id <> ''6001''';
    execute immediate sql_text into lon1;
    
    select GRID.LAT_TO_ROW(lat1)-11 into row_bottom from dual;
    select GRID.LON_TO_COL(lon1)-6 into col_left from dual;
    sql_text := 'select trunc(GRID.ROW_TO_BOTTOM_LAT(' || row_bottom ||'), ' || accuracy || ') from dual';
    execute immediate sql_text into lat1;
    sql_text := 'select trunc(GRID.COL_TO_LEFT_LON(' || col_left ||'), ' || accuracy || ') from dual';
    execute immediate sql_text into lon1;
    sql_text := 'select GRID.LAT_TO_ROW(''' || lat1 || ''') from dual';
    execute immediate sql_text into row_bottom;
    sql_text := 'select GRID.LON_TO_COL(''' || lon1 || ''') from dual';
    execute immediate sql_text into col_left;
    
    lat2_tmp := lat1;
    lon2_tmp := lon1;
    
    sql_text := 'select max(latitude)  from ' || bsTable || ' where site_id <> ''6001''';
    execute immediate sql_text into lat2; 
    sql_text := 'select max(longitude) from ' || bsTable || ' where site_id <> ''6001''';
    execute immediate sql_text into lon2;
    
    select value into step from grid_params where name='size_types';
    
    sq_num_lat := 0;
    LOOP
      select GRID.ROW_TO_TOP_LAT(row_bottom + sq_num_lat) into lat2_tmp from dual;
      IF (lat2_tmp >= lat2) THEN
        EXIT;
      ELSE
        sq_num_lat := sq_num_lat+power(2,step-1);
      END IF;
    END LOOP;
    lat2 := lat2_tmp;
    
    sq_num_lon := 0;
    LOOP
      select GRID.COL_TO_right_LON(col_left + sq_num_lon) into lon2_tmp from dual;
      IF (lon2_tmp >= lon2) THEN
        EXIT;
      ELSE
        sq_num_lon := sq_num_lon+power(2,step);
      END IF;
    END LOOP;
    lon2 := lon2_tmp;
    
    select GRID.COUNT_BY_COORDS(lat1, lon1, lat2, lon2, 'cell_id') into count_cell_id from dual;
    select GRID.COUNT_BY_COORDS(lat1, lon1, lat2, lon2, 'site_id') into count_site_id from dual;
  
    sql_text := ' INSERT INTO ' || nTable || '
      (LAT1, LAT2, LON1, LON2, LABEL, COUNT_CELL_ID, DONE, SQ_NUM_LAT, SQ_NUM_LON, ROW_BOTTOM, COL_LEFT, COUNT_SITE_ID, CLUSTER_LEVEL)
    values
      (
        trunc(''' || lat1 || ''', ' || accuracy || '),
        trunc(''' || lat2 || ''', ' || accuracy || '),
        trunc(''' || lon1 || ''', ' || accuracy || '),
        trunc(''' || lon2 || ''', ' || accuracy || '),
        1, 
        ' || count_cell_id || ', 
        ''N'', 
        ' || sq_num_lat || ', 
        ' || sq_num_lon || ', 
        ' || row_bottom || ',
        ' || col_left || ', 
        ' || count_site_id || ', 
        0
      ) ';
    execute immediate sql_text;
    commit;
    
    --make backup
    sql_text := 'select count(*) from user_tables where table_name=''' || nTable || '_BK''';
    execute immediate sql_text into if_exists;
    IF if_exists > 0 THEN
      sql_text := 'DROP TABLE ' || nTable || '_BK';
      execute immediate sql_text;
    END IF;
    
    sql_text := 'CREATE TABLE ' || nTable || '_BK as select * from ' || nTable;
    execute immediate sql_text;
  END GRID_INITIAL_ROW;
  
  
  --main procedure
  PROCEDURE CALC_GRID AS 
    row_bottom      NUMBER;
    col_left        NUMBER;
    max_value       NUMBER;
    max_clusters    NUMBER;
    cur_cells       NUMBER; --значение количества сот в текущем кластере
    temp_clusters   NUMBER; --промежуточное значение кластеров
    Ns              NUMBER;
    line_exists     NUMBER;
    sessions        NUMBER;
    nTable          VARCHAR2(100);
    sql_text        VARCHAR2(1000);
  BEGIN
    select value        into max_value    from grid_params where name = 'max_value';
    select value        into max_clusters from grid_params where name = 'max_clusters';
    select value_string into nTable       from grid_params where name='grid_table_name';
    
    FOR CURR IN (
      select * from grid_source_test order by cluster_level 
    )
    LOOP
      sql_text := 'select count(*) from ' || nTable || ' where count_cell_id>0';
      execute immediate sql_text into temp_clusters;

      sql_text := 'select count(*) from ' || nTable || ' where label =' || curr.label;
      execute immediate sql_text into line_exists;
      if line_exists =0 then
        CONTINUE;
      end if;
        
      sql_text := 'select count_cell_id from ' || nTable || ' where label =' || curr.label;
      execute immediate sql_text into cur_cells;
      
      sql_text := 'select count(*) from ' || nTable || ' where done=''N''';
      execute immediate sql_text into Ns;
      
      IF (temp_clusters >= max_clusters) then
        IF (Ns = 0) THEN
          return;
        ELSE
          update grid_source_test set done='Y';
          commit;
          return;
        END IF;
      ELSIF cur_cells <= max_value then
        update grid_source_test set done='Y' where label = curr.label;
        commit;
      END IF;
      IF (Ns = 0) THEN
        return;
      END IF;
    
      IF curr.done <> 'Y' then
        IF curr.count_site_id <> 1 then
          select GRID.lat_to_row(curr.lat1) into row_bottom from dual;
          select GRID.lon_to_col(curr.lon1) into col_left from dual;
          
          GRID.DIVIDE_BY_COORDS(row_bottom, col_left, curr.sq_num_lat, curr.sq_num_lon, curr.label, curr.cluster_level, curr.count_cell_id, curr.count_site_id, max_value);
          GRID.CALC_GRID(); --рекурсия
        ELSE
          update grid_source_test set done='Y' where label = curr.label;
          commit;
        END IF;
      END IF;
    END LOOP;
  END CALC_GRID;
  
  PROCEDURE DIVIDE_BY_COORDS(
    row_bottom    NUMBER,
    col_left      NUMBER,
    sq_num_lat    NUMBER,
    sq_num_lon    NUMBER,
    nLabel        VARCHAR2,
    cluster_level NUMBER,
    count_cell_id NUMBER,
    count_site_id NUMBER,
    max_value     NUMBER
  ) AS 
    prob_count_vert1       NUMBER;
    min_value              NUMBER;
    prob_count_vert2       NUMBER;
    prob_count_hori1       NUMBER;
    prob_count_hori2       NUMBER;
    rate1                  NUMBER;
    rate2                  NUMBER;
    site_count1            NUMBER;
    site_count2            NUMBER;
    sql_text               VARCHAR2(1000); 
    nTable                 VARCHAR(50);
    lat1                   NUMBER;
    lat2                   NUMBER;
    lat3                   NUMBER;
    lon1                   NUMBER;
    lon2                   NUMBER;
    lon3                   NUMBER;
    first_check            BOOLEAN;
    least_less_hori        BOOLEAN;
    least_less_vert        BOOLEAN;
    greatest_less_hori     BOOLEAN;
    greatest_less_vert     BOOLEAN;
    is_not_sensible_hori   BOOLEAN;
    is_not_sensible_vert   BOOLEAN;
  BEGIN
    select value into min_value from grid_params where name='min_value';
    select value_string into nTable  from grid_params where name='grid_table_name';
    sql_text := 'select lat1, lon1, lat2, lon2 from ' || nTable || ' gs where gs.label = ' || nLabel;
    EXECUTE IMMEDIATE sql_text into lat1, lon1, lat3, lon3;
    
    sql_text := 'select GRID.ROW_TO_top_LAT(GRID.LAT_TO_ROW(''' || to_char(lat1) || ''')+''' || to_char(sq_num_lat/2) || ''') from dual' ;
    EXECUTE IMMEDIATE sql_text into lat2;
    sql_text := 'select GRID.COL_TO_RIGHT_LON(GRID.LON_TO_COL(''' || to_char(lon1) || ''')+''' || to_char(sq_num_lon/2) || ''') from dual' ;
    EXECUTE IMMEDIATE sql_text into lon2;

    select GRID.count_by_coords(lat1, lon1, lat2, lon3, 'cell_id') into prob_count_hori1 from dual;
    select GRID.count_by_coords(lat2, lon1, lat3, lon3, 'cell_id') into prob_count_hori2 from dual;
    if greatest(prob_count_hori1,prob_count_hori2) = 0 then
      update grid_source_test set done='Y' where label = nLabel;
      commit;
      return;
    else
      rate1 := abs(1 - least(prob_count_hori1,prob_count_hori2)/greatest(prob_count_hori1,prob_count_hori2));
    end if;
    
    select GRID.count_by_coords(lat1, lon1, lat3, lon2, 'cell_id') into prob_count_vert1 from dual;
    select GRID.count_by_coords(lat1, lon2, lat3, lon3, 'cell_id') into prob_count_vert2 from dual;
    if greatest(prob_count_vert1,prob_count_vert2) = 0 then
--      rate2 := 0;
      update grid_source_test set done='Y' where label = nLabel;
      commit;
      return;
    else
      rate2 := abs(1 - least(prob_count_vert1,prob_count_vert2)/greatest(prob_count_vert1,prob_count_vert2));
    end if;

    IF (count_cell_id <= max_value or count_site_id =1)                   then first_check := true;        Else first_check := false; end if;
    IF (least(prob_count_hori1,prob_count_hori2) between 1 and min_value) then least_less_hori := true;    Else least_less_hori := false; end if;
    IF (least(prob_count_vert1,prob_count_vert2) between 1 and min_value) then least_less_vert := true;    Else least_less_vert := false; end if;
    IF (greatest(prob_count_hori1,prob_count_hori2) < max_value)          then greatest_less_hori := true; Else greatest_less_hori := false; end if;
    IF (greatest(prob_count_vert1,prob_count_vert2) < max_value)          then greatest_less_vert := true; Else greatest_less_vert := false; end if;
    
    is_not_sensible_hori := false;
    is_not_sensible_vert := false;
    IF sq_num_lat > sq_num_lon then
      if (rate1 <>1 and (is_not_sensible_hori or (first_check or least_less_hori and greatest_less_hori))) or REMAINDER( sq_num_lat, 2 ) <>0 then
        update grid_source_test set done='Y' where label = nLabel;
        commit;
        return;
      end if;
      GRID.divide_cluster(nLabel, sq_num_lat, sq_num_lon, cluster_level, 'hori', lat1, lat2, lat3, lon1, lon2, lon3);
    ELSIF sq_num_lat < sq_num_lon then
      if (rate2 <>1 and (is_not_sensible_vert or (first_check or least_less_vert and greatest_less_vert))) or REMAINDER( sq_num_lon, 2 ) <>0 then
        update grid_source_test set done='Y' where label = nLabel;
        commit;
        return;
      end if;
      GRID.divide_cluster(nLabel, sq_num_lat, sq_num_lon, cluster_level, 'vert', lat1, lat2, lat3, lon1, lon2, lon3);
    ELSE
      if ((rate1 <>1 and rate2 <>1) and (first_check)) then
        update grid_source_test set done='Y' where label = nLabel;
        commit;
        return;
      end if;
      IF rate1=1 then
        IF REMAINDER( sq_num_lat, 2 ) <>0 or least_less_hori and greatest_less_hori or is_not_sensible_hori then
          update grid_source_test set done='Y' where label = nLabel;
          commit;
          return;
        END IF;
        GRID.divide_cluster(nLabel, sq_num_lat, sq_num_lon, cluster_level, 'hori', lat1, lat2, lat3, lon1, lon2, lon3);   
      elsif rate2=1 then
        IF REMAINDER( sq_num_lon, 2 ) <>0  or least_less_vert and greatest_less_vert or is_not_sensible_vert then
          update grid_source_test set done='Y' where label = nLabel;
          commit;
          return;
        END IF;
        GRID.divide_cluster(nLabel, sq_num_lat, sq_num_lon, cluster_level, 'vert', lat1, lat2, lat3, lon1, lon2, lon3);   
      elsif rate1 < rate2 then
        IF REMAINDER( sq_num_lat, 2 ) <>0 or least_less_hori and greatest_less_hori or is_not_sensible_hori then
          update grid_source_test set done='Y' where label = nLabel;
          commit;
          return;
        END IF;
        GRID.divide_cluster(nLabel, sq_num_lat, sq_num_lon, cluster_level, 'hori', lat1, lat2, lat3, lon1, lon2, lon3);        
      Else
        IF REMAINDER( sq_num_lon, 2 ) <>0  or least_less_vert and greatest_less_vert or is_not_sensible_vert then
          update grid_source_test set done='Y' where label = nLabel;
          commit;
          return;
        END IF;
        GRID.divide_cluster(nLabel, sq_num_lat, sq_num_lon, cluster_level, 'vert', lat1, lat2, lat3, lon1, lon2, lon3);
      END IF;
    END IF;
  END DIVIDE_BY_COORDS;

  PROCEDURE DIVIDE_CLUSTER(
    nLabel          VARCHAR2,
    sq_num_lat      NUMBER,
    sq_num_lon      NUMBER,
    cluster_level   NUMBER,
    div_type        VARCHAR2,
    nlat1           NUMBER,
    nlat2           NUMBER,
    nlat3           NUMBER,
    nlon1           NUMBER,
    nlon2           NUMBER,
    nlon3           NUMBER
  ) AS 
    lat1            NUMBER;
    lat2            NUMBER;
    lat3            NUMBER;
    lon1            NUMBER;
    lon2            NUMBER;
    lon3            NUMBER;
    lat4            NUMBER;
    lon4            NUMBER;
    count_cell_id1  NUMBER;
    count_cell_id2  NUMBER;
    count_site_id1  NUMBER;
    count_site_id2  NUMBER;
    sq_num_lat_new  NUMBER;
    sq_num_lon_new  NUMBER;
    sql_text        VARCHAR2(1000); 
--    nTable          VARCHAR(50);
    accuracy        NUMBER;
    row_bottom      NUMBER;
    col_left        NUMBER;
  BEGIN
    select value into accuracy from grid_params where name='accuracy';

    lat1 := nlat1;
    lat2 := nlat2;
    lat3 := nlat3;
    
    lon1 := nlon1;
    lon2 := nlon2;
    lon3 := nlon3;
        
    IF div_type = 'hori' then
      sq_num_lat_new := sq_num_lat/2;
      sq_num_lon_new := sq_num_lon;
      

      lat4 := lat3;--don't ask
      lat3 := lat2;
      ----------------------
      lon2 := lon3;
      lon4 := lon3;
      lon3 := lon1;
    ELSE
      sq_num_lat_new := sq_num_lat;
      sq_num_lon_new := sq_num_lon/2;
      lat2 := lat3;
      lat4 := lat3;
      lat3 := lat1;
      
      lon4 := lon3;
      lon3 := lon2;
    end if;  
      select GRID.LAT_TO_ROW(lat1) into row_bottom from dual;
      select GRID.LON_TO_COL(lon1) into col_left from dual;
      select COUNT_BY_COORDS(lat1, lon1, lat2, lon2, 'cell_id') into count_cell_id1 from dual;
      select COUNT_BY_COORDS(lat3, lon3, lat4, lon4, 'cell_id') into count_cell_id2 from dual;
      select COUNT_BY_COORDS(lat1, lon1, lat2, lon2, 'site_id') into count_site_id1 from dual;
      select COUNT_BY_COORDS(lat3, lon3, lat4, lon4, 'site_id') into count_site_id2 from dual;
      
    sql_text := 'insert into grid_source_test(lat1, lat2, lon1, lon2, label, count_cell_id, done, sq_num_lat, sq_num_lon, count_site_id, cluster_level, row_bottom, col_left)
      values (
        trunc(''' || lat1 || ''',' || accuracy || '),
        trunc(''' || lat2 || ''',' || accuracy || '),
        trunc(''' || lon1 || ''',' || accuracy || '),
        trunc(''' || lon2 || ''',' || accuracy || '),
        (select max(to_number(label))+1 from grid_source_test),
        ' || count_cell_id1 || ',
        ''N'',
        ' || sq_num_lat_new || ',
        ' || sq_num_lon_new || ',
        ' || count_site_id1 || ',
        ' || cluster_level  || '+1,
        ' || row_bottom     || ',
        ' || col_left       || '        
      )';
--      DBMS_OUTPUT.PUT_LINE(sql_text);
      EXECUTE IMMEDIATE sql_text;
      commit;

      select GRID.LAT_TO_ROW(lat3) into row_bottom from dual;
      select GRID.LON_TO_COL(lon3) into col_left from dual;
    sql_text := 'insert into grid_source_test(lat1, lat2, lon1, lon2, label, count_cell_id, done, sq_num_lat, sq_num_lon, count_site_id, cluster_level, row_bottom, col_left)
      values (
        trunc(''' || lat3 || ''',' || accuracy || '),
        trunc(''' || lat4 || ''',' || accuracy || '),
        trunc(''' || lon3 || ''',' || accuracy || '),
        trunc(''' || lon4 || ''',' || accuracy || '),
        (select max(to_number(label))+1 from grid_source_test),
        ' || count_cell_id2 || ',
        ''N'',
        ' || sq_num_lat_new || ',
        ' || sq_num_lon_new || ',
        ' || count_site_id2 || ',
        ' || cluster_level || '+1,
        ' || row_bottom     || ',
        ' || col_left       || ' 
      )';

      EXECUTE IMMEDIATE sql_text;
      commit;
      delete from grid_source_test gs where gs.label = nLabel;      
      commit;
  END DIVIDE_CLUSTER;
  
  PROCEDURE GRID_CALC_RESULTS AS
    if_exists     NUMBER;
    nTable        VARCHAR2(100);
    rTable        VARCHAR2(100);
    sql_text      VARCHAR2(1000);
    accuracy      NUMBER;
    param_value   NUMBER; 
    cDate         DATE;
  BEGIN
    select value_string into nTable  from grid_params where name='grid_table_name';
    select value_string into rTable  from grid_params where name='result_table_name';
    sql_text := 'select count(*) from user_tables where table_name=''' || rTable || '''';
    execute immediate sql_text into if_exists;
      IF if_exists = 0 THEN
        sql_text := q'[
          CREATE TABLE "lab"."]' || rTable || q'[" 
            (	
              "NAME" VARCHAR2(50 BYTE), 
              "VALUE" NUMBER, 
              "DESCRIPTION" VARCHAR2(1000 BYTE), 
              "CALC_DATE" DATE
            )]';
          execute immediate sql_text;
      END IF;
      select value into accuracy from grid_params where name='accuracy';
      cDate := systimestamp;
      for param in (
        select * from GRID_STATISTICS_PARAMS
      )
      LOOP
        sql_text := 'select round(' || param.value_string || ',' || accuracy || ') from ' || nTable || ' where count_cell_id>0';
        execute immediate sql_text into param_value;
        sql_text := q'[
          INSERT INTO ]' || rTable || q'[(NAME, VALUE, DESCRIPTION, CALC_DATE) 
            VALUES (
              ']' || param.name        || q'[',
              ']' || param_value       || q'[',
              ']' || param.description || q'[',
              ']' || cDate             || q'['
        )]';
        
        execute immediate sql_text;
        
      END LOOP;
  END GRID_CALC_RESULTS;
  
  PROCEDURE COMBINE_CLUSTERS_MANUAL(
    nLabel1         VARCHAR2,
    nLabel2         VARCHAR2
  ) AS 
    sql_text        VARCHAR2(1000); 
    nTable          VARCHAR(50);
    lat1            NUMBER;
    lat2            NUMBER;
    lon1            NUMBER;
    lon2            NUMBER;
    count_cell_id   NUMBER;
    sq_num_lat      NUMBER;
    sq_num_lon      NUMBER;
    count_site_id   NUMBER;
    cluster_level   NUMBER;
    row_bottom      NUMBER;
    col_left        NUMBER;
  BEGIN
    select value_string into nTable from grid_params where name='grid_table_name';
    sql_text := q'[
      select 
       min(lat1), 
        min(lon1), 
        max(lat2), 
        max(lon2), 
        sum(count_cell_id), 
        sum(count_site_id), 
        min(cluster_level), 
        min(row_bottom), 
        min(col_left),
        min(sq_num_lat),
        min(sq_num_lon)
      from ]' || nTable || q'[ 
    where 
      label in (]' || nLabel1 || ',' || nLabel2 || ')';
    EXECUTE IMMEDIATE sql_text into lat1, lon1, lat2, lon2, count_cell_id, count_site_id, cluster_level, row_bottom, col_left, sq_num_lat, sq_num_lon;
    sql_text := q'[
      insert into ]' || nTable || q'[ (lat1, lat2, lon1, lon2, label, count_cell_id, done, sq_num_lat, sq_num_lon, count_site_id, cluster_level, row_bottom, col_left) 
       values (
        ']' || lat1 || q'[',
        ']' || lat2 || q'[',
        ']' || lon1 || q'[',
        ']' || lon2 || q'[',
        (select max(to_number(label))+1 from ]' || nTable || q'[),
        ']' || count_cell_id || q'[',
        'Y',
        ']' || sq_num_lat || q'[',
        ']' || sq_num_lon || q'[',
        ']' || count_site_id || q'[',
         ]' || cluster_level || q'[+1,
        ']' || row_bottom || q'[',
        ']' || col_left   || q'['
        
       )
    ]';

    EXECUTE IMMEDIATE sql_text;
    sql_text := 'delete from ' || nTable || ' where label in (' || nLabel1 || ',' || nLabel2 || ')';
    EXECUTE IMMEDIATE sql_text;
  END COMBINE_CLUSTERS_MANUAL;
  
  PROCEDURE DIVIDE_CLUSTER_MANUAL(
    nLabel          VARCHAR2,
    div_type        VARCHAR2
  ) AS 
    sq_num_lat      NUMBER;
    sq_num_lon      NUMBER;
    cluster_level   NUMBER;
    lat1            NUMBER;
    lat2            NUMBER;
    lat3            NUMBER;
    lon1            NUMBER;
    lon2            NUMBER;
    lon3            NUMBER;
    sql_text        VARCHAR2(1000); 
    nTable          VARCHAR(50);
    row_bottom    NUMBER;
    row_top       NUMBER;
    col_left      NUMBER;
    col_right     NUMBER;
  BEGIN
      select value_string into nTable from grid_params where name='grid_table_name';
      sql_text := 'select sq_num_lat, sq_num_lon, cluster_level from ' || nTable || ' where label=''' || nLabel || '''';
      EXECUTE IMMEDIATE sql_text into sq_num_lat, sq_num_lon, cluster_level;
      sql_text := 'select lat1, lon1, lat2, lon2 from ' || nTable || ' gs where gs.label = ' || nLabel;
      EXECUTE IMMEDIATE sql_text into lat1, lon1, lat3, lon3;
      
      col_left   := GRID.lon_to_col(lon1);
      col_right  := GRID.lon_to_col(lon3);
      row_bottom := GRID.lat_to_row(lat1);
      row_top    := GRID.lat_to_row(lat3);
      sq_num_lat := row_top - row_bottom;
      sq_num_lon := col_right - col_left;
      
      sql_text := 'select GRID.ROW_TO_top_LAT(GRID.LAT_TO_ROW(''' || to_char(lat1) || ''')+''' || to_char(sq_num_lat/2) || ''') from dual' ;
      EXECUTE IMMEDIATE sql_text into lat2;
      sql_text := 'select GRID.COL_TO_RIGHT_LON(GRID.LON_TO_COL(''' || to_char(lon1) || ''')+''' || to_char(sq_num_lon/2) || ''') from dual' ;
      EXECUTE IMMEDIATE sql_text into lon2;
      
      GRID.divide_cluster(nLabel, sq_num_lat, sq_num_lon, cluster_level, div_type, lat1, lat2, lat3, lon1, lon2, lon3);
      
  END DIVIDE_CLUSTER_MANUAL;
  
  PROCEDURE EXPAND_CLUSTER_MANUAL(
    nLabel        VARCHAR2,
    wwhere        VARCHAR2
  ) AS 
    sql_text      VARCHAR2(1000); 
    lat1          NUMBER;
    lat2          NUMBER;
    lon1          NUMBER;
    lon2          NUMBER;
    sq_num_lat    NUMBER;
    sq_num_lon    NUMBER;
    row_bottom    NUMBER;
    row_top       NUMBER;
    col_left      NUMBER;
    col_right     NUMBER;
    accuracy      NUMBER;
  BEGIN
    select value into accuracy from grid_params where name='accuracy';
    for curr in (
      select * from grid_source_test where label = nLabel
    )
    loop
      col_left   := GRID.lon_to_col(curr.lon1);
      col_right  := GRID.lon_to_col(curr.lon2);
      row_bottom := GRID.lat_to_row(curr.lat1);
      row_top    := GRID.lat_to_row(curr.lat2);
      lat1 := curr.lat1;  
      lat2 := curr.lat2;
      lon1 := curr.lon1;
      lon2 := curr.lon2;
      sq_num_lat := row_top - row_bottom;
      sq_num_lon := col_right - col_left;
      IF wwhere = 'top'       then
        sq_num_lat := sq_num_lat*2;
        lat2 := trunc(GRID.row_to_top_lat(row_bottom + sq_num_lat), accuracy);
      elsif wwhere = 'right'  then
        sq_num_lon := sq_num_lon*2;
        lon2 := trunc(GRID.col_to_right_lon(col_left + sq_num_lon), accuracy);
      elsif wwhere = 'left'   then
        sq_num_lon := sq_num_lon*2;
        lon1 := trunc(GRID.col_to_left_lon(col_right - sq_num_lon+1), accuracy);
      elsif wwhere = 'bottom' then
        sq_num_lat := sq_num_lat*2;
        lat1 := trunc(GRID.row_to_top_lat(row_top - sq_num_lat), accuracy);
      end if;
      sql_text := q'[
        update grid_source_test
        set
          lat1 = ']' || lat1 || q'[',
          lat2 = ']' || lat2 || q'[',
          lon1 = ']' || lon1 || q'[',
          lon2 = ']' || lon2 || q'[',
          count_cell_id = ]' || curr.count_cell_id || q'[,
          count_site_id = ]' || curr.count_site_id || q'[,
          sq_num_lat = ]' || sq_num_lat || q'[,
          sq_num_lon = ]' || sq_num_lon || q'[,
          row_bottom = ]' || row_bottom || q'[,
          col_left = ]'   || col_left || q'[,
          cluster_level = ]' || to_number(curr.cluster_level+1) || q'[,
          done = 'Y'        
        where
          label = ']' || nLabel || q'[']';
      EXECUTE IMMEDIATE sql_text;
      commit;
    end loop;
  END EXPAND_CLUSTER_MANUAL;
  
  PROCEDURE FAST_RESTART AS 
  BEGIN
  
    execute immediate 'truncate table cem_grid_new_test';
    execute immediate q'[insert into cem_grid_new_test
    select lat1,lat2,lon1,lon2,label, '123' from grid_source_test where count_cell_id>0]';
    commit;
    
    execute immediate 'truncate table cem_grid_values_new_test';
    execute immediate q'[insert into cem_grid_values_new_test
    select label,0,0,0,0,0,0,count_cell_id,count_cell_id,0,0,'123', sysdate from grid_source_test where count_cell_id>0]';
    commit;
    
    execute immediate 'update cem_grid_values_new_test g1 set g1.color_priority = round(abs(200 - g1.color_priority)*1/20,1)  ';
    commit;
  END FAST_RESTART;
  
  PROCEDURE POST_PROCESSING AS 
    nTable          VARCHAR2(100);
    max_value       NUMBER;
    sql_text        VARCHAR2(1000); 
  BEGIN
    select value_string into nTable from grid_params where name='grid_table_name';
    select value        into max_value    from grid_params where name = 'max_value';
    
    FOR CURR IN (
      select 
        gs1.label as label1,
        gs2.label as label2,
        gs2.lat1 as lat1,
        gs1.lat2 as lat2,
        gs1.lon1 as lon1,
        gs1.lon2 as lon2,
        gs1.count_cell_id + gs2.count_cell_id as count_cell_id,
        gs1.count_site_id + gs2.count_site_id as count_site_id,
        gs1.sq_num_lat + gs2.sq_num_lat as sq_num_lat,
        gs1.sq_num_lon as sq_num_lon,
        gs2.row_bottom as row_bottom,
        gs2.col_left as col_left,
        gs1.cluster_level-1 as cluster_level,
        'Y' as done
      from 
        ( select * from grid_source_test where COUNT_CELL_ID between 1 and 300) gs1
      left join 
        ( select * from grid_source_test where COUNT_CELL_ID between 1 and 300) gs2
      on 
        gs1.lat1 = gs2.lat2 and gs1.lon1 = gs2.lon1 and gs1.lon2 = gs2.lon2
      where 
        (
          gs1.sq_num_lat = gs1.sq_num_lon 
          and gs2.sq_num_lat = gs2.sq_num_lon
          or
          gs1.sq_num_lat < gs1.sq_num_lon 
          and gs2.sq_num_lat < gs2.sq_num_lon
        )
        and gs2.lat1 is not null
        and (abs(200 - (gs1.count_cell_id + gs2.count_cell_id)) < (abs(200 - gs1.count_cell_id) + abs(200 - gs2.count_cell_id))/2 )  
    )
    Loop
      sql_text := 'insert into grid_source_test(lat1, lat2, lon1, lon2, label, count_cell_id, done, sq_num_lat, sq_num_lon, count_site_id, cluster_level, row_bottom, col_left)
        values (
          ''' || curr.lat1 || ''',
          ''' || curr.lat2 || ''',
          ''' || curr.lon1 || ''',
          ''' || curr.lon2 || ''',
          (select max(to_number(label))+1 from grid_source_test),
          ' || curr.COUNT_CELL_ID || ',
          ''Y'',
          ' || curr.sq_num_lat || ',
          ' || curr.sq_num_lon || ',
          ' || curr.count_site_id || ',
          ' || curr.cluster_level  || ',
          ' || curr.row_bottom     || ',
          ' || curr.col_left       || '        
        )';

        EXECUTE IMMEDIATE sql_text;
        sql_text := 'delete from grid_source_test where label=' || curr.label1;
                EXECUTE IMMEDIATE sql_text;
        
        sql_text := 'delete from grid_source_test where label=' || curr.label2;
                EXECUTE IMMEDIATE sql_text;
                commit;
    end loop;
    
    
    
    FOR CURR IN (
      select 
        gs1.label as label1,
        gs2.label as label2,
        gs2.lat1 as lat1,
        gs1.lat2 as lat2,
        gs2.lon1 as lon1,
        gs1.lon2 as lon2,
        gs1.count_cell_id + gs2.count_cell_id as count_cell_id,
        gs1.count_site_id + gs2.count_site_id as count_site_id,
        gs1.sq_num_lat as sq_num_lat,
        gs1.sq_num_lon + gs2.sq_num_lon as sq_num_lon,
        gs2.row_bottom as row_bottom,
        gs2.col_left as col_left,
        gs1.cluster_level-1 as cluster_level,
        'Y' as done
      from 
        ( select * from grid_source_test where COUNT_CELL_ID between 1 and 300) gs1
      left join 
        ( select * from grid_source_test where COUNT_CELL_ID between 1 and 300) gs2
      on 
        gs1.lon1 = gs2.lon2 and gs1.lat1 = gs2.lat1 and gs1.lat2 = gs2.lat2
      where
        (
          gs1.sq_num_lat = gs1.sq_num_lon 
          and gs2.sq_num_lat = gs2.sq_num_lon
          or
          gs1.sq_num_lat > gs1.sq_num_lon
          and gs2.sq_num_lat > gs2.sq_num_lon
        )
        and gs2.lat1 is not null
        and (abs(200  - (gs1.count_cell_id + gs2.count_cell_id)) < (abs(200 - gs1.count_cell_id) + abs(200 - gs2.count_cell_id))/2 ) 
    )
    Loop
      sql_text := 'insert into grid_source_test(lat1, lat2, lon1, lon2, label, count_cell_id, done, sq_num_lat, sq_num_lon, count_site_id, cluster_level, row_bottom, col_left)
        values (
          ''' || curr.lat1 || ''',
          ''' || curr.lat2 || ''',
          ''' || curr.lon1 || ''',
          ''' || curr.lon2 || ''',
          (select max(to_number(label))+1 from grid_source_test),
          ' || curr.COUNT_CELL_ID || ',
          ''Y'',
          ' || curr.sq_num_lat || ',
          ' || curr.sq_num_lon || ',
          ' || curr.count_site_id || ',
          ' || curr.cluster_level  || ',
          ' || curr.row_bottom     || ',
          ' || curr.col_left       || '        
        )';

        EXECUTE IMMEDIATE sql_text;
        commit;
        sql_text := 'delete from grid_source_test where label=' || curr.label1;

        EXECUTE IMMEDIATE sql_text;
        sql_text := 'delete from grid_source_test where label=' || curr.label2;

        EXECUTE IMMEDIATE sql_text;
    end loop;
  END POST_PROCESSING;  
  
  FUNCTION COUNT_BY_COORDS(
      lat1        NUMBER, 
      lon1        NUMBER, 
      lat2        NUMBER, 
      lon2        NUMBER,
      nCol        VARCHAR2
    ) RETURN NUMBER AS 
      cur_count   NUMBER;
      sql_text    VARCHAR2(1000);
      bsTable     VARCHAR2(100);
    BEGIN
      sql_text := 'select value_string from grid_params where name=''bts_table_name''';
      execute immediate sql_text into bsTable;
      sql_text := q'[
        select count(*) from
        ( select 
            distinct ]' || nCol || q'[ ,lac 
          from 
            ]' || bsTable || q'[ 
          where 
                latitude >=  ']' || lat1 || ''' and  latitude  <''' || lat2 || q'['
            and longitude >= ']' || lon1 || ''' and  longitude <''' || lon2 || q'['
        )
        ]';
      execute immediate sql_text into cur_count;
        
      return cur_count;
    END COUNT_BY_COORDS;
  
  FUNCTION DIVIDE_CLUSTER_WEB(
    nLabel          VARCHAR2,
    div_type        VARCHAR2
  )
  RETURN tbldivclus 
  AS
    l_data_table tbldivclus := tbldivclus();
    sq_num_lat      NUMBER;
    sq_num_lon      NUMBER;
    cluster_level   NUMBER;
    lat1            NUMBER;
    lat2            NUMBER;
    lat3            NUMBER;
    lon1            NUMBER;
    lon2            NUMBER;
    lon3            NUMBER;
    sql_text        VARCHAR2(1000); 
    nTable          VARCHAR(50);
    row_bottom    NUMBER;
    row_top       NUMBER;
    col_left      NUMBER;
    col_right     NUMBER;
  BEGIN
      select value_string into nTable from grid_params where name='grid_table_name';
      sql_text := 'select sq_num_lat, sq_num_lon, cluster_level from ' || nTable || ' where label=''' || nLabel || '''';
      EXECUTE IMMEDIATE sql_text into sq_num_lat, sq_num_lon, cluster_level;
      sql_text := 'select lat1, lon1, lat2, lon2 from ' || nTable || ' gs where gs.label = ' || nLabel;
      EXECUTE IMMEDIATE sql_text into lat1, lon1, lat3, lon3;
      
      col_left   := GRID.lon_to_col(lon1);
      col_right  := GRID.lon_to_col(lon3);
      row_bottom := GRID.lat_to_row(lat1);
      row_top    := GRID.lat_to_row(lat3);
      sq_num_lat := row_top - row_bottom;
      sq_num_lon := col_right - col_left;
      
      sql_text := 'select GRID.ROW_TO_top_LAT(GRID.LAT_TO_ROW(''' || to_char(lat1) || ''')+''' || to_char(sq_num_lat/2) || ''') from dual' ;
      EXECUTE IMMEDIATE sql_text into lat2;
      sql_text := 'select GRID.COL_TO_RIGHT_LON(GRID.LON_TO_COL(''' || to_char(lon1) || ''')+''' || to_char(sq_num_lon/2) || ''') from dual' ;
      EXECUTE IMMEDIATE sql_text into lon2;
      
      GRID.divide_cluster(nLabel, sq_num_lat, sq_num_lon, cluster_level, div_type, lat1, lat2, lat3, lon1, lon2, lon3);
      for curr in 
      (
                  select 
          to_char(cgv.calc_date,'dd.mm.yyyy') as calc_date,
          cg.label,
          cg.address,
          cg.lat1 lat1,
          cg.lon1 lon1,
          cg.lat1 lat2,
          cg.lon2 lon2,
          cg.lat2 lat3,
          cg.lon2 lon3,
          cg.lat2 lat4,
          cg.lon1 lon4,
          round(cgv.rr_sum/1000000,2) as rr_sum,
          round(cgv.gm_sum/1000000,2) as gm_sum,
          cgv.tqi_sum,
          cgv.rr,
          cgv.gm,
          cgv.tqi,
          cgv.color_priority,
          cgv.sectors, 
          cgv.red_sectors,
          cgv.work_sectors
        from 
            ( select * from lab.cem_grid_values_new_test where calc_date=(select max(calc_date) from lab.cem_grid_values_new_test) ) cgv
        left join
            ( select * from lab.cem_grid_new_test ) cg
        on
            cgv.label = cg.label
        where
          cg.label = (select max(to_number(label)) from lab.cem_grid_values_new_test)
          or cg.label = (select max(to_number(label))-1 from lab.cem_grid_values_new_test)
        order by 
            color_priority desc
      )
      loop
        L_DATA_TABLE.EXTEND;
        l_data_table(l_data_table.count) := (ROWDIVCLUS(
                                                          curr.calc_date, 
                                                          curr.label, 
                                                          curr.address, 
                                                          curr.lat1, 
                                                          curr.lon1, 
                                                          curr.lat2, 
                                                          curr.lon2,
                                                          curr.lat3,
                                                          curr.lon3,
                                                          curr.lat4,
                                                          curr.lon4,
                                                          curr.rr_sum,
                                                          curr.gm_sum,
                                                          curr.tqi_sum,
                                                          curr.rr,
                                                          curr.gm,
                                                          curr.tqi,
                                                          curr.color_priority,
                                                          curr.sectors,
                                                          curr.red_sectors,
                                                          curr.work_sectors
                                            )) ;
      end loop;
      execute immediate 'truncate table cem_grid_new_test';
      execute immediate q'[insert into cem_grid_new_test
      select lat1,lat2,lon1,lon2,label, '123' from grid_source_test where count_cell_id>0]';
      commit;
      
      execute immediate 'truncate table cem_grid_values_new_test';
      execute immediate q'[insert into cem_grid_values_new_test
      select label,0,0,0,0,0,0,count_cell_id,count_cell_id,0,0,'123', sysdate from grid_source_test where count_cell_id>0]';
      commit;
      
      execute immediate 'update cem_grid_values_new_test g1 set g1.color_priority = round(abs(100 - g1.color_priority)*0.1,1)  ';
      commit;
      return l_data_table;
  END DIVIDE_CLUSTER_WEB;
---------------------------------------------------------
  FUNCTION COL_TO_LEFT_LON(
    col number) 
    RETURN Number AS 
    resolution NUMBER; --18 для квадрата со стороной 90м
    pi NUMBER;
    lon NUMBER;
    
    BEGIN
      select value into resolution from grid_params where name = 'resolution';
      select value into pi from grid_params where name = 'pi';
      select
        col * power(2,-resolution) * 360 
      into 
        lon
      from dual;
    return lon;
  END COL_TO_LEFT_LON;
  
  FUNCTION COL_TO_RIGHT_LON(
      col number) 
      RETURN Number AS 
      resolution NUMBER; --18 для квадрата со стороной 90м
      pi NUMBER;
      lon NUMBER;
      
    BEGIN
      select value into resolution from grid_params where name = 'resolution';
      select value into pi from grid_params where name = 'pi';
      select
        (col+1) * power(2,-resolution) * 360 
      into 
        lon
      from dual;
    return lon;
  END COL_TO_RIGHT_LON;
  
  FUNCTION LAT_TO_ROW(
      lat number) 
      RETURN Number AS 
      resolution NUMBER; --18 для квадрата со стороной 90м
      pi NUMBER;
      row NUMBER;
      
    BEGIN
      select value into resolution from grid_params where name = 'resolution';
      select value into pi from grid_params where name = 'pi';
      select
        floor(ln(tan((lat / 2 + 45) * pi / 180)) / (2 * pi) * power(2,resolution))
      into 
        row
      from dual;
    return row;
    END LAT_TO_ROW;
    
  FUNCTION LON_TO_COL(
      lon number) 
      RETURN Number AS 
      resolution NUMBER; --18 для квадрата со стороной 90м
      pi NUMBER;
      col NUMBER;
      
    BEGIN
      select value into resolution from grid_params where name = 'resolution';
      select value into pi from grid_params where name = 'pi';
      select
        floor(MOD(lon,360)/ 360 * power(2,resolution) )
      into 
        col
      from dual;
    return col;
    END LON_TO_COL;
    
  FUNCTION ROW_TO_BOTTOM_LAT(
      row number) 
      RETURN Number AS 
      resolution NUMBER; --18 для квадрата со стороной 90м
      pi NUMBER;
      lat NUMBER;
      
    BEGIN
      select value into resolution from grid_params where name = 'resolution';
      select value into pi from grid_params where name = 'pi';
      select
        2 * atan(exp( row * power(2,-resolution) * 2 * pi )) * 180 / pi - 90
      into 
        lat
      from dual;
    return lat;
    END ROW_TO_BOTTOM_LAT;
  
  FUNCTION ROW_TO_TOP_LAT(
      row number) 
      RETURN Number AS 
      resolution NUMBER; --18 для квадрата со стороной 90м
      pi NUMBER;
      lat NUMBER;
      
    BEGIN
      select value into resolution from grid_params where name = 'resolution';
      select value into pi         from grid_params where name = 'pi';
      select
        2 * atan(exp((row + 1) * power(2,-resolution) * 2 * pi)) * 180 / pi - 90 
      into 
        lat
      from dual;
    return lat;
    END ROW_TO_TOP_LAT;
    
END GRID;

/
