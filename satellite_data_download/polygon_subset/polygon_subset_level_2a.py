import rasterio
import numpy
import json
from pathlib import Path
import zipfile
from shapely import wkt
from fiona.crs import CRS
import geopandas as gpd
from shapely.geometry import box
from rasterio.mask import mask
from send2trash import send2trash

from satellite_data_download.polygon_subset.abstract_polygon_subset import (
    AbstractPolygonSubset,
)


class PolygonSubsetLevel2A(AbstractPolygonSubset):
    """
    Create susbset of zipfile downloaded by the Sentinel2Api. The polygon used for the subset and
    where it will be saved is handle by the PolygonFolderManager.
    New complete zip file is saved with every ".jp2" file croped according to the polygon.
    The new file is now smaller and is also georeferenced according to his new representation.
    """

    @property
    def polygon_epsg(self):
        return 4326

    @property
    def target_epsg(self):
        return 32620

    def _create_mask_coords_from_wkt_polygon(
        self,
        wkt_polygon,
    ):
        polygone_boudary = numpy.dstack(wkt.loads(wkt_polygon).boundary.xy)[0]
        min_x = polygone_boudary[:, 0].min()
        max_x = polygone_boudary[:, 0].max()
        min_y = polygone_boudary[:, 1].min()
        max_y = polygone_boudary[:, 1].max()
        bbox = box(min_x, min_y, max_x, max_y)

        geo = gpd.GeoDataFrame(
            {"geometry": bbox}, index=[0], crs=CRS.from_epsg(self.polygon_epsg)
        )
        geo = geo.to_crs(self.target_epsg)

        coords = [json.loads(geo.to_json())["features"][0]["geometry"]]

        return coords

    def _crop_and_create_new_file_according_to_wkt_polygon(
        self,
        to_split_path,
        destination_path,
        filename,
        wkt_polygon,
    ):
        to_split_zip_path = Path(to_split_path, filename)
        destination_zip_path = Path(destination_path, filename)
        if destination_zip_path.exists():
            raise ValueError(f"file {filename} already exist")

        # Copy all the ZipFile except for the .jp2 files.
        # Those file are croped before copied
        with zipfile.ZipFile(to_split_zip_path, "r") as original_zip:
            with zipfile.ZipFile(destination_zip_path, "w") as destination_zip:
                for item in original_zip.infolist():
                    if "jp2" not in item.filename:
                        data = original_zip.read(item.filename)
                        destination_zip.writestr(item, data)

        coords = self._create_mask_coords_from_wkt_polygon(wkt_polygon)

        zip_path = str(Path(to_split_zip_path))
        zip_format_path = f"zip+file:{zip_path}!"
        with zipfile.ZipFile(Path(zip_path), "r") as f:
            all_file_name = f.filelist

        jp2_files = list(filter(lambda file: "jp2" in file.filename, all_file_name))
        with zipfile.ZipFile(destination_zip_path, "a") as zip_archive:
            for jp2 in jp2_files:
                raster = rasterio.open(
                    Path(zip_format_path, jp2.filename), driver="JP2OpenJPEG"
                )
                out_raster, out_transform = mask(raster, shapes=coords, crop=True)
                out_meta = raster.meta.copy()
                out_meta.update(
                    {
                        "height": out_raster.shape[1],
                        "width": out_raster.shape[2],
                        "transform": out_transform,
                    }
                )
                raster.close()
                with rasterio.open("tmp.jp2", "w", **out_meta) as dest:
                    dest.write(out_raster)
                zip_archive.write("tmp.jp2", jp2.filename)
                send2trash("tmp.jp2")
