# grid

Разбиение на кластеры множества точек на карте.
Кластеры должны удовлетворять следующим требованиям:
 - Каждая точка имеет свой вес (точка - это Базовая Станция, ее вес - количество сот на ней), в каждый кластер должно попадать примерно одинаковое количество сот, но не меньше и не больше некоторой величины;
 - Кластер представляет собой либо прямоугольник с соотношением сторон 2:1/1:2, либо квадрат;
 - Линии кластеров должны строго накладываться на сетку, в свою очередь координаты которой вычисляются по формулам, описанным в функциях (COL_TO_LEFT_LON, COL_TO_RIGHT_LON, LAT_TO_ROW, LON_TO_COL, ROW_TO_BOTTOM_LAT, ROW_TO_TOP_LAT);
 - Кластеры, внутри которых нет точек, не записываются в итоговую таблицу;
 - Точки в исходную таблицу постоянно добавляются, удаляются, а у оставшихся пересчитываются веса. Должна быть возможность как пересчитать с минимальными изменениями уже посчитанные кластеры, так и рассчитать их заново.
 
Приведенный код представляет собой PL/SQL Package, написанный под Oracle 11g.
На входе принимается таблица с точками, их весами и координатами, а также в случае пересчета таблица с уже посчитанными ранее координатами кластеров.
На выходе создается новая таблица с координатами кластеров.

Примеры разбиения для Московской области со случайными точками представлены на картинках: mo.png и mo_close_highlight.png