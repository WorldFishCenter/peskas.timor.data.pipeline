import pandas as pd
from keplergl import KeplerGl 

def kepler_map(data_path):
    df = pd.read_csv(data_path)
    config = {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [],
                "layers": [
                    {
                        "id": "fmrn4wj",
                        "type": "hexagon",
                        "config": {
                            "dataId": "dat_tracks",
                            "label": "PDS data",
                            "color": [255, 203, 153],
                            "highlightColor": [252, 242, 26, 255],
                            "columns": {
                                "lat": "Lat",
                                "lng": "Lng"
                            },
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 1,
                                "worldUnitSize": 0.9,
                                "resolution": 8,
                                "colorRange": {
                                    "name": "Pink Wine 6",
                                    "type": "sequential",
                                    "category": "Uber",
                                    "colors": [
                                        "#2C1E3D",
                                        "#573660",
                                        "#83537C",
                                        "#A6758E",
                                        "#C99DA4",
                                        "#EDD1CA"
                                    ]
                                },
                                "coverage": 1,
                                "sizeRange": [0, 500],
                                "percentile": [0, "100"],
                                "elevationPercentile": [0, "500"],
                                "elevationScale": 54.5,
                                "enableElevationZoomFactor": True,
                                "colorAggregation": "average",
                                "sizeAggregation": "sum",
                                "enable3d": True
                            },
                            "hidden": False,
                            "textLabel": [
                                {
                                    "field": None,
                                    "color": [255, 255, 255],
                                    "size": 18,
                                    "offset": [0, 0],
                                    "anchor": "start",
                                    "alignment": "center"
                                }
                            ]
                        },
                        "visualChannels": {
                            "colorField": {
                                "name": "GPS tracks",
                                "type": "integer"
                            },
                            "colorScale": "quantile",
                            "sizeField": {
                                "name": "GPS tracks",
                                "type": "integer"
                            },
                            "sizeScale": "sqrt"
                        }
                    }
                ],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "pm64h5f61": [
                                {"name": "Lng", "format": None},
                                {"name": "Lat", "format": None},
                                {"name": "GPS tracks", "format": None}
                            ]
                        },
                        "compareMode": False,
                        "compareType": "absolute",
                        "enabled": True
                    },
                    "brush": {
                        "size": 0.5,
                        "enabled": False
                    },
                    "geocoder": {
                        "enabled": False
                    },
                    "coordinate": {
                        "enabled": False
                    }
                },
                "layerBlending": "additive",
                "splitMaps": [],
                "animationConfig": {
                    "currentTime": None,
                    "speed": 1
                }
            },
            "mapState": {
                "bearing": 10.625,
                "dragRotate": True,
                "latitude": -8.800818125103254,
                "longitude": 125.49043470312294,
                "pitch": 39.80299785867238,
                "zoom": 7.891150147692834,
                "isSplit": False
            },
            "mapStyle": {
                "styleType": "dark",
                "topLayerGroups": {
                    "label": True,
                    "road": False,
                    "border": True,
                    "building": False,
                    "water": False,
                    "land": False
                },
                "visibleLayerGroups": {
                    "label": True,
                    "road": True,
                    "border": True,
                    "building": False,
                    "water": True,
                    "land": False,
                    "3d building": False
                },
                "threeDBuildingColor": [9.665468314072013, 17.18305478057247, 31.1442867897876],
                "mapStyles": {}
            }
        }
    }    
    kpmap = KeplerGl(height=400)
    kpmap.add_data(data=df, name='dat_tracks')
    kpmap.config = config
    kpmap = KeplerGl(height=400, data={'dat_tracks': df}, config=config)
    kpmap.save_to_html(data={'dat_tracks': df}, config=config, file_name='kepler_pds_map.html', read_only = "True")

    
    
