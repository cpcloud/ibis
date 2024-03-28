from __future__ import annotations

from ibis.formats.pandas import PandasData


class BigQueryPandasData(PandasData):
    @classmethod
    def convert_GeoSpatial(cls, s, dtype, pandas_type):
        import geopandas as gpd
        import shapely as shp

        return gpd.GeoSeries(shp.from_wkt(s))

    convert_Point = convert_LineString = convert_Polygon = convert_MultiLineString = (
        convert_MultiPoint
    ) = convert_MultiPolygon = convert_GeoSpatial

    @classmethod
    def convert_Map(cls, s, dtype, pandas_type):
        raw_json_objects = cls.convert_JSON(s, dtype, pandas_type)
        return super().convert_Map(raw_json_objects, dtype, pandas_type)

    @classmethod
    def convert_JSON(cls, s, dtype, pandas_type):
        converter = cls.convert_JSON_element(dtype)
        return s.map(converter, na_action="ignore").astype("object")

    @classmethod
    def convert_Array(cls, s, dtype, pandas_type):
        if dtype.value_type.is_json():
            s = cls.convert_JSON(s, dtype, pandas_type)
        return super().convert_Array(s, dtype, pandas_type)
