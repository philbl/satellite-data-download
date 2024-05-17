from datetime import date
import getpass
import winshell

from satellite_data_download.sentinel_data_downloader import SentinelDataDownloader
from satellite_data_download.models.sentinel_api_query_input import (
    SentinelAPIQueryInput,
)

from satellite_data_download.polygon_folder_manager.polygon_folder_manager_factory import (
    PolygonFolderManagerFactory,
)
from satellite_data_download.polygon_subset.polygon_subset_factory import (
    PolygonSubsetFactory,
)
from satellite_data_download.atmospheric_correction.acolite_atmospheric_correction import (
    AcoliteAtmoshpericCorrection,
)


USER = "philippe.blouin-leclerc@inrs.ca"  # input("UserName: ")
PASSWORD = getpass.getpass()
GREAT_AREA_LIST = [
    # "nb_bouctouche_cocagne",
    # "ipe_dunk_west",
    "ipe_morell",
]
YEAR_LIST = [2019, 2018]
PROCESSING_LEVEL = "level-1c"


if __name__ == "__main__":
    for great_area in GREAT_AREA_LIST:
        for year in YEAR_LIST:
            print(f"Downloading {great_area} {year}")
            for i in range(18):
                polygon_folder_manager = (
                    PolygonFolderManagerFactory.create_polygon_folder_manager(
                        great_area, PROCESSING_LEVEL
                    )
                )
                sentinel_api_query_input = SentinelAPIQueryInput(
                    area=polygon_folder_manager.download_polygon,
                    date=(date(year, 4, 1), date(year, 11, 1)),
                    platformname="SENTINEL-2",
                    processinglevel=PROCESSING_LEVEL,
                    cloudcoverpercentage=(0, 100),
                )

                sentinel_data_downloader = SentinelDataDownloader(
                    USER, PASSWORD, sentinel_api_query_input, polygon_folder_manager
                )

                sentinel_data_downloader.download_online_uuid(5)
                polygon_folder_manager.apply_download_post_processing()
                if len(list(winshell.recycle_bin())) > 0:
                    winshell.recycle_bin().empty(
                        confirm=False, show_progress=False, sound=False
                    )

                if PROCESSING_LEVEL == "level-1c":
                    # # Apply Atmospheric Correction
                    atmospheric_correction = AcoliteAtmoshpericCorrection(
                        polygon_folder_manager
                    )
                    atmospheric_correction.generate_bash_file()
                    atmospheric_correction.run_bash_file()
                    atmospheric_correction.delete_corrected_files()

                polygon_subset = PolygonSubsetFactory.create_polygon_subset(
                    polygon_folder_manager, PROCESSING_LEVEL
                )
                polygon_subset.apply_polygon_subset_from_to_split_path_to_destination_list()
                polygon_folder_manager.delete_file_in_to_split_path_that_are_in_destination_list()

                if len(list(winshell.recycle_bin())) > 0:
                    winshell.recycle_bin().empty(
                        confirm=False, show_progress=False, sound=False
                    )
