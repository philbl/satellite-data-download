def get_folder_management_dict(processing_level):
    assert processing_level in ["level-1c", "level-2a"]
    processing_level = processing_level.replace("-", "_")
    processing_level_folder = f"../data/sentinel2_data/{processing_level}"
    to_split_path = "/atmospheric_correction" if processing_level == "level_1c" else ""
    return {
        "nb_bouctouche_cocagne": {
            "download_path": f"{processing_level_folder}/nb_bouctouche_cocagne",
            "to_split_path": f"{processing_level_folder}/nb_bouctouche_cocagne{to_split_path}",
            "destination_list": [
                {
                    "estuary_name": "bouctouche",
                    "destination_path": f"{processing_level_folder}/bouctouche",
                },
                {
                    "estuary_name": "cocagne",
                    "destination_path": f"{processing_level_folder}/cocagne",
                },
            ],
        },
        "ipe_dunk_west": {
            "download_path": f"{processing_level_folder}/ipe_dunk_west",
            "to_split_path": f"{processing_level_folder}/ipe_dunk_west{to_split_path}",
            "destination_list": [
                {
                    "estuary_name": "dunk",
                    "destination_path": f"{processing_level_folder}/dunk",
                },
                {
                    "estuary_name": "west",
                    "destination_path": f"{processing_level_folder}/west",
                },
            ],
        },
        "ipe_morell": {
            "download_path": f"{processing_level_folder}/ipe_morell",
            "to_split_path": f"{processing_level_folder}/ipe_morell{to_split_path}",
            "destination_list": [
                {
                    "estuary_name": "morell",
                    "destination_path": f"{processing_level_folder}/morell",
                }
            ],
        },
    }
