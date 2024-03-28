from abc import ABC, abstractmethod
from pathlib import Path
from tqdm import tqdm
from satellite_data_download.polygon_folder_manager.abstract_polygon_folder_manager import (
    AbstractPolygonFolderManager,
)


class AbstractPolygonSubset(ABC):
    def __init__(self, polygon_folder_manager: AbstractPolygonFolderManager):
        assert isinstance(polygon_folder_manager, AbstractPolygonFolderManager)
        self.polygon_folder_manager = polygon_folder_manager

    @abstractmethod
    def _crop_and_create_new_file_according_to_wkt_polygon(
        self, to_split_path, destination_path, filename, wkt_polygon
    ):
        """ """

    def apply_polygon_subset_from_to_split_path_to_destination_list(self):
        to_split_path = self.polygon_folder_manager.to_split_path
        for destination_dict in self.polygon_folder_manager.destination_list:
            destination_path = destination_dict["destination_path"]
            wkt_polygon = destination_dict["polygon"]
            filename_list = [
                filename.name
                for filename in Path(to_split_path).iterdir()
                if not Path(destination_path, filename.name).exists()
            ]
            for filename in tqdm(filename_list):
                self._crop_and_create_new_file_according_to_wkt_polygon(
                    to_split_path,
                    destination_path,
                    filename,
                    wkt_polygon,
                )
