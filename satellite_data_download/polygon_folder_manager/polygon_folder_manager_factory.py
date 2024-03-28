from satellite_data_download.polygon_folder_manager.polygon_folder_manager_level_1c import (
    PolygonFolderManagerLevel1C,
)
from satellite_data_download.polygon_folder_manager.polygon_folder_manager_level_2a import (
    PolygonFolderManagerLevel2A,
)


class PolygonFolderManagerFactory:
    @staticmethod
    def create_polygon_folder_manager(great_area_name, processing_level):
        if processing_level == "level-1c":
            return PolygonFolderManagerLevel1C(great_area_name, processing_level)
        elif processing_level == "level-2a":
            return PolygonFolderManagerLevel2A(great_area_name, processing_level)
        else:
            raise ValueError("Invalid processing level specified.")
