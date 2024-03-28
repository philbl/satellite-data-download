import numpy
from pathlib import Path
from shapely import wkt
import sys

from satellite_data_download.polygon_subset.abstract_polygon_subset import (
    AbstractPolygonSubset,
)

sys.path.append("../acolite/")
from acolite.output import crop_acolite_netcdf  # noqa E402


class PolygonSubsetLevel1C(AbstractPolygonSubset):
    """
    Create susbset of zipfile downloaded by the Sentinel2Api. The polygon used for the subset and
    where it will be saved is handle by the PolygonFolderManager.
    New complete zip file is saved with every ".jp2" file croped according to the polygon.
    The new file is now smaller and is also georeferenced according to his new representation.
    """

    def _get_limit_polygon_acolite_list(self, wkt_polygon):
        polygone_boudary = numpy.dstack(wkt.loads(wkt_polygon).boundary.xy)[0]
        min_lat = polygone_boudary[:, 0].min()
        max_lat = polygone_boudary[:, 0].max()
        min_lon = polygone_boudary[:, 1].min()
        max_lon = polygone_boudary[:, 1].max()
        limit_polygon_acolite = [min_lon, min_lat, max_lon, max_lat]

        return limit_polygon_acolite

    def _crop_and_create_new_file_according_to_wkt_polygon(
        self,
        to_split_path,
        destination_path,
        filename,
        wkt_polygon,
    ):
        to_split_nc_path = Path(to_split_path, filename)
        destination_nc_path = Path(destination_path, filename)
        if destination_nc_path.exists():
            raise ValueError(f"file {filename} already exist")

        limit_polygon_acolite = self._get_limit_polygon_acolite_list(wkt_polygon)
        crop_acolite_netcdf(
            to_split_nc_path, output=destination_nc_path, limit=limit_polygon_acolite
        )
