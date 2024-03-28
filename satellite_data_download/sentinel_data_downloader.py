import requests
from sentinelsat.exceptions import InvalidChecksumError, ServerError
from datetime import datetime
import numpy
import pandas
import time
from pathlib import Path
from tqdm import tqdm

from satellite_data_download.models.sentinel_api_query_input import (
    SentinelAPIQueryInput,
)
from satellite_data_download.polygon_folder_manager.abstract_polygon_folder_manager import (
    AbstractPolygonFolderManager,
)


class SentinelDataDownloader:
    # _URL = "https://apihub.copernicus.eu/apihub"
    FILTER_NAME_PROCESSING_LEVEL_DICT = {"level-2a": "MSIL2A", "level-1c": "MSIL1C"}

    def __init__(
        self,
        username: str,
        password: str,
        sentinel_api_query_input: SentinelAPIQueryInput,
        polygon_folder_manager: AbstractPolygonFolderManager,
    ):
        self.username = username
        self.password = password
        self.polygon_folder_manager = polygon_folder_manager
        self.download_directory_path = Path(self.polygon_folder_manager.download_path)
        self.sentinel_api_query_input = sentinel_api_query_input
        self.products_df = self._get_products_df()

    def _get_products_df(self, retries=3):
        data_collection = self.sentinel_api_query_input.platformname
        processing_level = self.sentinel_api_query_input.processinglevel
        processing_level_filter = self.FILTER_NAME_PROCESSING_LEVEL_DICT[
            processing_level
        ]
        aoi = self.sentinel_api_query_input.area
        start_date = self.sentinel_api_query_input.date[0]
        end_date = self.sentinel_api_query_input.date[1]
        for i in range(retries):
            try:
                json = requests.get(
                    f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name "
                    f"eq '{data_collection}' "
                    f"and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}') and "
                    f"ContentDate/Start gt {start_date}T00:00:00.000Z and "
                    f"ContentDate/Start lt {end_date}T00:00:00.000Z&$count=True&$top=1000"
                ).json()
                products_df = pandas.DataFrame.from_dict(json["value"])
            except KeyError:
                if i < retries - 1:
                    print(f"retry Nb: {i+1}")
                    continue
                else:
                    raise
            else:
                break
        products_df = products_df[
            products_df["Name"].apply(lambda name: "_N05" in name or "_N04" in name)
        ].reset_index(drop=True)
        products_df = products_df[
            products_df["Name"].apply(lambda name: processing_level_filter in name)
        ].reset_index(drop=True)
        return products_df

    def _get_not_downloaded_uuid_isonline_dict(self, verbose=1):
        uuid_dict = {}
        for _, row in self.products_df.iterrows():
            uuid = row["Id"]
            folder_name = row["Name"].split(".")[0]
            is_online = row["Online"]
            if (
                self.polygon_folder_manager.determine_if_folder_zip_is_already_downloaded(
                    folder_name
                )
                is False
            ):
                uuid_dict[uuid] = {}
                uuid_dict[uuid]["is_online"] = is_online
                uuid_dict[uuid]["folder_name"] = folder_name
        if verbose >= 1:
            print(f"Number of uuid: {len(self.products_df)}")
            print(f"Number of uuid to download: {len(uuid_dict)}")
            print(
                "Number of online uuid: "
                f"{numpy.sum([single_dict['is_online'] for single_dict in list(uuid_dict.values())])}"
            )
        self.uuid_dict = uuid_dict

    def trigger_download_of_offline_uuid(self, sleeping_time=1920):
        self._get_not_downloaded_uuid_isonline_dict()
        number_of_trigger = 0
        for uuid, is_online in self.uuid_dict.items():
            if is_online is False:
                number_of_trigger += 1
                try:
                    self.api.download(
                        id=uuid, directory_path=str(self.download_directory_path)
                    )
                except Exception as e:
                    print(
                        f"{number_of_trigger}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {e}"
                    )
                    time.sleep(sleeping_time)

    def download_online_uuid(self, max_uuid_to_download=100):
        self._get_not_downloaded_uuid_isonline_dict()
        number_of_download = 0
        number_to_download = min(
            max_uuid_to_download,
            int(
                numpy.sum(
                    [
                        single_dict["is_online"]
                        for single_dict in list(self.uuid_dict.values())
                    ]
                )
            ),
        )
        for uuid, sub_dict in self.uuid_dict.items():
            if number_of_download >= max_uuid_to_download:
                break
            is_online = sub_dict["is_online"]
            folder_name = sub_dict["folder_name"]
            if is_online:
                number_of_download += 1
                print(f"{number_of_download}/{number_to_download}")
                try:
                    access_token = get_access_token(self.username, self.password)
                    url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({uuid})/$value"
                    headers = {"Authorization": f"Bearer {access_token}"}

                    session = requests.Session()
                    session.headers.update(headers)
                    response = session.get(url, headers=headers, stream=True)

                    with open(
                        Path(self.download_directory_path, f"{folder_name}.zip"), "wb"
                    ) as file:
                        for chunk in tqdm(
                            response.iter_content(chunk_size=8192),
                            total=int(int(response.headers["Content-Length"]) / 8192),
                        ):
                            if chunk:
                                file.write(chunk)
                except InvalidChecksumError:
                    print(f"InvalidChecksumError {uuid}")
                except ServerError:
                    # ServerError when downloading that file for ipe_dunk_west
                    # S2B_MSIL2A_20230905T151659_N0509_R025_T20TMS_20230905T202528.zip
                    if uuid == "cba3415a-4faa-4a63-9106-babf9ddcada1":
                        continue
                    else:
                        raise ServerError


def get_access_token(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
        r.raise_for_status()
    except Exception:
        raise Exception(
            f"Access token creation failed. Reponse from the server was: {r.json()}"
        )
    return r.json()["access_token"]
