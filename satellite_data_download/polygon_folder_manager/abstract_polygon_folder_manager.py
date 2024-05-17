from abc import ABC, abstractmethod
from pathlib import Path
from send2trash import send2trash

from satellite_data_download.config.polygon_dict import POLYGON_DICT
from satellite_data_download.config.folder_management import get_folder_management_dict


class AbstractPolygonFolderManager(ABC):
    def __init__(self, great_area_name, processing_level):
        self.great_area_name = great_area_name
        self.folder_management_dict = get_folder_management_dict(processing_level)
        self._download_path = self._get_download_path()
        self._to_split_path = self._get_to_split_path()
        self._destination_list = self._get_destination_list()
        self._download_polygon = self._get_download_polygon()

    def _validate_greate_area_name(self, great_area_name):
        assert great_area_name in [
            "nb_bouctouche_cocagne",
            "ipe_dunk_west",
            "ipe_morell",
        ]

    @property
    def download_path(self):
        return self._download_path

    @property
    def to_split_path(self):
        return self._to_split_path

    @property
    def destination_list(self):
        return self._destination_list

    @property
    def download_polygon(self):
        return self._download_polygon

    def _get_download_path(self):
        return self.folder_management_dict[self.great_area_name]["download_path"]

    def _get_to_split_path(self):
        return self.folder_management_dict[self.great_area_name]["to_split_path"]

    def _get_destination_list(self):
        destination_list = self.folder_management_dict[self.great_area_name][
            "destination_list"
        ]
        for destination in destination_list:
            destination_name = destination["estuary_name"]
            destination["polygon"] = POLYGON_DICT[destination_name]
        return destination_list

    def _get_download_polygon(self):
        polygon = self.destination_list[0]["polygon"]
        return polygon

    def delete_file_in_to_split_path_that_are_in_destination_list(self):
        """
        Delete the zip file in the to split path, if the file is now present in all the
        destination path list
        """
        file_list = [file for file in Path(self.to_split_path).iterdir()]
        for file in file_list:
            filename = file.name
            if all(
                [
                    Path(destination_dict["destination_path"], filename).is_file()
                    for destination_dict in self.destination_list
                ]
            ):
                send2trash(file)

    @abstractmethod
    def apply_download_post_processing(self):
        """ """

    @abstractmethod
    def determine_if_folder_zip_is_already_downloaded(self, folder_name):
        """ """
