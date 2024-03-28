from datetime import date
import numpy
import pandas

from satellite_data_download.sentinel_data_downloader import SentinelDataDownloader
from satellite_data_download.models.sentinel_api_query_input import (
    SentinelAPIQueryInput,
)
from satellite_data_download.polygon_folder_manager.polygon_folder_manager_factory import (
    PolygonFolderManagerFactory,
)

USER = "NOT_NEEDED"
PASSWORD = "NOT_NEEDED"
GREAT_AREA_LIST = [
    "nb_bouctouche_cocagne",
    # "ipe_dunk_west",
    # "ipe_morell",
]  # Choice are ["nb_bouctouche_cocagne", "ipe_dunk_west", "ipe_morell"]
YEAR_LIST = [2022, 2023]  # , 2023]
PROCESSING_LEVEL = "level-2a"


if __name__ == "__main__":
    df = pandas.DataFrame(
        columns=[
            "Area",
            "Year",
            "Nbr of Product",
            "Nbr of Product to Download",
            "Nbr of Online Product",
            "Nbr Product to Trigger",
            "Estimate Time to Trigger All",
        ]
    )
    total_trigger_time = 0
    i = 0
    for year in YEAR_LIST:
        for area in GREAT_AREA_LIST:
            polygon_folder_manager = (
                PolygonFolderManagerFactory.create_polygon_folder_manager(
                    area, PROCESSING_LEVEL
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
            sentinel_data_downloader._get_not_downloaded_uuid_isonline_dict(verbose=0)

            number_of_product = len(sentinel_data_downloader.products_df)
            number_of_product_to_download = len(sentinel_data_downloader.uuid_dict)
            number_of_online_product = int(
                numpy.sum(
                    [
                        single_dict["is_online"]
                        for single_dict in list(
                            sentinel_data_downloader.uuid_dict.values()
                        )
                    ]
                )
            )
            number_of_product_to_trigger = (
                number_of_product_to_download - number_of_online_product
            )
            estimate_time_to_trigger_all_in_min = number_of_product_to_trigger * 32
            estimate_time_to_trigger_all_in_hour_str = (
                f"{int(estimate_time_to_trigger_all_in_min/60)}h:"
                f"{estimate_time_to_trigger_all_in_min%60}m"
            )
            total_trigger_time += estimate_time_to_trigger_all_in_min

            df.loc[i, "Area"] = area
            df.loc[i, "Year"] = year
            df.loc[i, "Nbr of Product"] = number_of_product
            df.loc[i, "Nbr of Product to Download"] = number_of_product_to_download
            df.loc[i, "Nbr of Online Product"] = number_of_online_product
            df.loc[i, "Nbr Product to Trigger"] = number_of_product_to_trigger
            df.loc[
                i, "Estimate Time to Trigger All"
            ] = estimate_time_to_trigger_all_in_hour_str

            i += 1
    df.loc[i, "Area"] = "All"
    df.loc[i, "Year"] = "-".join(str(year) for year in YEAR_LIST)
    df.loc[i, "Nbr of Product"] = int(df.loc[:i, "Nbr of Product"].sum())
    df.loc[i, "Nbr of Product to Download"] = int(
        df.loc[:i, "Nbr of Product to Download"].sum()
    )
    df.loc[i, "Nbr of Online Product"] = int(df.loc[:i, "Nbr of Online Product"].sum())
    df.loc[i, "Nbr Product to Trigger"] = int(
        df.loc[:i, "Nbr Product to Trigger"].sum()
    )
    df.loc[
        i, "Estimate Time to Trigger All"
    ] = f"{int(total_trigger_time/60)}h:{total_trigger_time%60}m"

    print(df)
