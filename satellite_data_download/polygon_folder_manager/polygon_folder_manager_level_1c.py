from pathlib import Path
import zipfile
from send2trash import send2trash

from satellite_data_download.polygon_folder_manager.abstract_polygon_folder_manager import (
    AbstractPolygonFolderManager,
)


class PolygonFolderManagerLevel1C(AbstractPolygonFolderManager):
    """
    Class to handle the folder management when downloading data via Sentinel2 API.
    When downloading Sentinel2 data, a polygon is used to make the query, then the image downloaded
    contains that polygon but also a way bigger image. So for two differents polygon, it
    might return the same big images.
    This class handle to retrieve where the original data will be downloaded (source path) and where it will
    be saved when it's gonna be split by the src.satellite_data.polygon_subset.polygon_subset.PolygonSubset
    It also handle how to delete the file in the source path when it has been split in the destination list.
    """

    def _unzip_file(self, zip_folder_path):
        extract_dir = Path(self.download_path).absolute()
        with zipfile.ZipFile(zip_folder_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        send2trash(Path(zip_folder_path).resolve())

    def apply_download_post_processing(self):
        absolute_source_path = Path(self.download_path).absolute()
        zip_folder_list = list(
            filter(lambda path: path.suffix == ".zip", absolute_source_path.iterdir())
        )
        for zip_folder_path in zip_folder_list:
            self._unzip_file(zip_folder_path)

    def determine_if_folder_zip_is_already_downloaded(self, folder_name):
        folder_name_exist = False
        # In Download Path
        for suffix in [".zip", ".SAFE"]:
            folder_name_exist = (
                folder_name_exist
                | Path(self.download_path, f"{folder_name}{suffix}").exists()
            )
        # In to split path
        date = folder_name.split("_")[2]
        folder_name_date = f"{date[:4]}_{date[4:6]}_{date[6:8]}"
        all_filename_in_to_split_path = [
            path.name for path in Path(self.to_split_path).iterdir()
        ]
        folder_name_exist = folder_name_exist | any(
            [folder_name_date in filename for filename in all_filename_in_to_split_path]
        )

        # In destination Path
        destination_path_exists = []
        for destination_dict in self.destination_list:
            destination_path = destination_dict["destination_path"]
            all_filename_in_destination_path = [
                path.name for path in Path(destination_path).iterdir()
            ]
            destination_path_exists.append(
                any(
                    [
                        folder_name_date in filename
                        for filename in all_filename_in_destination_path
                    ]
                )
            )
        folder_name_exist = folder_name_exist | all(destination_path_exists)

        return folder_name_exist
