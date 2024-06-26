from datetime import date
import getpass

from satellite_data_download.sentinel_data_downloader import SentinelDataDownloader
from satellite_data_download.models.sentinel_api_query_input import (
    SentinelAPIQueryInput,
)
from satellite_data_download.polygon_folder_manager.polygon_folder_manager_level_2a import (
    PolygonFolderManager,
)

USER = input("UserName: ")
PASSWORD = getpass.getpass()
GREAT_AREA_LIST = [
    "ipe_dunk_west"
]  # Choice are ["nb_bouctouche_cocagne", "ipe_dunk_west", "ipe_morell"]
YEAR_LIST = [2023]


if __name__ == "__main__":
    for great_area in GREAT_AREA_LIST:
        for year in YEAR_LIST:
            print(f"Tigger Downloading {great_area} {year}")
            polygon_folder_manager = PolygonFolderManager(great_area)
            sentinel_api_query_input = SentinelAPIQueryInput(
                area=polygon_folder_manager.download_polygon,
                date=(date(year, 5, 1), date(year, 11, 1)),
                platformname="Sentinel-2",
                processinglevel="Level-2A",
                cloudcoverpercentage=(0, 100),
            )

            sentinel_data_downloader = SentinelDataDownloader(
                USER, PASSWORD, sentinel_api_query_input, polygon_folder_manager
            )

            sentinel_data_downloader.trigger_download_of_offline_uuid()
