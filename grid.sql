create or replace PACKAGE     GRID AS 
  
  --создание начального кластера, на этот момент должна существовать заполненная таблица grid_params
  PROCEDURE GRID_INITIAL_ROW;
  
  --восстановить значения из бекап таблицы без пересчета
  PROCEDURE GRID_INITIAL_ROW_SHORT;
  
  --начальная рекурсивная функция
  PROCEDURE CALC_GRID;
  
  --подсчет статистики по получившимся результатам
  PROCEDURE GRID_CALC_RESULTS;
  
  --принятие решения о разбиении кластера
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
  );
  
  --разбиение кластера
  PROCEDURE DIVIDE_CLUSTER(
    nLabel          VARCHAR2,
    sq_num_lat      NUMBER,
    sq_num_lon      NUMBER,
    cluster_level   NUMBER,
    div_type        VARCHAR2,
    nlat1            NUMBER,
    nlat2            NUMBER,
    nlat3            NUMBER,
    nlon1            NUMBER,
    nlon2            NUMBER,
    nlon3            NUMBER
  );
  
  PROCEDURE COMBINE_CLUSTERS_MANUAL(
    nLabel1         VARCHAR2,
    nLabel2         VARCHAR2
  );
  
  PROCEDURE DIVIDE_CLUSTER_MANUAL(
    nLabel          VARCHAR2,
    div_type        VARCHAR2
  );
  
  PROCEDURE EXPAND_CLUSTER_MANUAL(
    nLabel        VARCHAR2,
    wwhere        VARCHAR2
  );
  
  PROCEDURE FAST_RESTART;
  PROCEDURE POST_PROCESSING;

  --подсчет количества сот в кластере
   FUNCTION COUNT_BY_COORDS(
      lat1        NUMBER, 
      lon1        NUMBER, 
      lat2        NUMBER, 
      lon2        NUMBER,
      nCol        VARCHAR2
    ) RETURN NUMBER;
    
    FUNCTION DIVIDE_CLUSTER_WEB(
      nLabel          VARCHAR2,
      div_type        VARCHAR2
    )
    RETURN tbldivclus;
  ------------------------------
  --номер колонки в долготу слева
  FUNCTION COL_TO_LEFT_LON(
    col number) 
    RETURN Number;
  
  --номер колонки в долготу справа
  FUNCTION COL_TO_RIGHT_LON(
      col number) 
      RETURN Number;
  
  --широту в ряд
  FUNCTION LAT_TO_ROW(
      lat number) 
      RETURN Number;
      
  --долготу в колонку
  FUNCTION LON_TO_COL(
      lon number) 
      RETURN Number;
      
  --ряд в нижнюю широту
  FUNCTION ROW_TO_BOTTOM_LAT(
      row number) 
      RETURN Number;
      
  --ряд в верхнюю широту      
  FUNCTION ROW_TO_TOP_LAT(
      row number) 
      RETURN Number;
    
END GRID;
