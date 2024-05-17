from pathlib import Path
import subprocess
from send2trash import send2trash

from satellite_data_download.polygon_folder_manager.abstract_polygon_folder_manager import (
    AbstractPolygonFolderManager,
)


class AcoliteAtmoshpericCorrection:
    ACOLITE_EXE_RELATIVE_PATH = "../acolite_py_win/dist/acolite/acolite.exe"
    ACOLITE_SETTING_RELATIVE_PATH = "../data/sentinel2_data/level_1c/acolite_setting"

    def __init__(self, polygon_folder_manager: AbstractPolygonFolderManager):
        assert isinstance(polygon_folder_manager, AbstractPolygonFolderManager)
        self.polygon_folder_manager = polygon_folder_manager
        self.great_area_name = polygon_folder_manager.great_area_name
        self.absolute_download_path = (
            Path(polygon_folder_manager.download_path).absolute().resolve()
        )
        self.str_absolute_to_split_path = str(
            Path(polygon_folder_manager.to_split_path).absolute().resolve()
        ).replace("\\", "/")
        self.str_absolute_acolite_exe_path = str(
            Path(self.ACOLITE_EXE_RELATIVE_PATH).absolute().resolve()
        ).replace("\\", "/")
        self.str_absolute_setting_path = str(
            Path(
                self.ACOLITE_SETTING_RELATIVE_PATH,
                f"acolite_setting_{self.great_area_name}.txt",
            )
            .absolute()
            .resolve()
        ).replace("\\", "/")
        self.output_sh_file = f"{self.great_area_name}.sh"

    def generate_bash_file(self):
        foldername_list = [
            path
            for path in filter(
                lambda path: path.suffix == ".SAFE",
                self.absolute_download_path.iterdir(),
            )
        ]
        self.corrected_folder_list = foldername_list
        basic_command = (
            f"'{self.str_absolute_acolite_exe_path}' --cli "
            f"--settings='{self.str_absolute_setting_path}' --inputfile='"
        )
        delete_file_command = (
            f"rm '{self.str_absolute_to_split_path}/'*_L1R.nc \n"
            f"rm '{self.str_absolute_to_split_path}/'*_L2R.nc \n"
            f"rm '{self.str_absolute_to_split_path}/'*.txt \n"
        )
        if Path(self.output_sh_file).exists():
            send2trash(self.output_sh_file)
        for foldername in foldername_list:
            with open(self.output_sh_file, "a") as f:
                command = basic_command + str(foldername).replace("\\", "/") + "' \n"
                f.write(command)
                f.write(delete_file_command)

    def run_bash_file(self):
        if Path(self.output_sh_file).exists():
            subprocess.run(f"bash {self.output_sh_file}")
            send2trash(self.output_sh_file)

    def delete_corrected_files(self):
        for folder_name in self.corrected_folder_list:
            send2trash(folder_name)
