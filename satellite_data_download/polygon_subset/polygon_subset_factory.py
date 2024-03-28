from satellite_data_download.polygon_subset.polygon_subset_level_1c import (
    PolygonSubsetLevel1C,
)
from satellite_data_download.polygon_subset.polygon_subset_level_2a import (
    PolygonSubsetLevel2A,
)


class PolygonSubsetFactory:
    @staticmethod
    def create_polygon_subset(polygon_folder_manager, processing_level):
        if processing_level == "level-1c":
            return PolygonSubsetLevel1C(polygon_folder_manager)
        elif processing_level == "level-2a":
            return PolygonSubsetLevel2A(polygon_folder_manager)
        else:
            raise ValueError("Invalid processing level specified.")
