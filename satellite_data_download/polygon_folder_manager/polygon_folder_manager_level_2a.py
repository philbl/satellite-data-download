from pathlib import Path

from satellite_data_download.polygon_folder_manager.abstract_polygon_folder_manager import (
    AbstractPolygonFolderManager,
)


class PolygonFolderManagerLevel2A(AbstractPolygonFolderManager):
    """
    Class to handle the folder management when downloading data via Sentinel2 API.
    When downloading Sentinel2 data, a polygon is used to make the query, then the image downloaded
    contains that polygon but also a way bigger image. So for two differents polygon, it
    might return the same big images.
    This class handle to retrieve where the original data will be downloaded (source path) and where it will
    be saved when it's gonna be split by the src.satellite_data.polygon_subset.polygon_subset.PolygonSubset
    It also handle how to delete the file in the source path when it has been split in the destination list.
    """

    def apply_download_post_processing(self):
        """ """
        pass

    def determine_if_folder_zip_is_already_downloaded(self, folder_name):
        is_file_in_download_path = Path(
            self.download_path, f"{folder_name}.zip"
        ).is_file()

        is_file_in_all_destination_path = all(
            [
                Path(
                    destination_dict["destination_path"], f"{folder_name}.zip"
                ).is_file()
                for destination_dict in self.destination_list
            ]
        )
        return is_file_in_download_path or is_file_in_all_destination_path
