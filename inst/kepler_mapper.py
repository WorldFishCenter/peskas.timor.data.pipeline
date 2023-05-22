import uvicorn
import pandas as pd
from keplergl import KeplerGl 
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

def kepler_map(data_path):
    df = pd.read_csv(data_path)
    config = {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [],
                "layers": [
                    {
                        "id": "8akleke",
                        "type": "hexagon",
                        "config": {
                            "dataId": "dat_tracks",
                            "label": "point",
                            "color": [255, 203, 153],
                            "highlightColor": [252, 242, 26, 255],
                            "columns": {
                                "lat": "Lat",
                                "lng": "Lng"
                            },
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.88,
                                "worldUnitSize": 0.9,
                                "resolution": 8,
                                "colorRange": {
                                    "name": "Ice And Fire",
                                    "type": "diverging",
                                    "category": "Uber",
                                    "colors": [
                                        "#0198BD",
                                        "#49E3CE",
                                        "#E8FEB5",
                                        "#FEEDB1",
                                        "#FEAD54",
                                        "#D50255"
                                    ]
                                },
                                "coverage": 1,
                                "sizeRange": [0, 473.68],
                                "percentile": [0, 100],
                                "elevationPercentile": [0, 100],
                                "elevationScale": 85.1,
                                "enableElevationZoomFactor": False,
                                "colorAggregation": "average",
                                "sizeAggregation": "sum",
                                "enable3d": True,
                                "legend": True
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
                                "name": "PDS tracks",
                                "type": "integer"
                            },
                            "colorScale": "quantile",
                            "sizeField": {
                                "name": "PDS tracks",
                                "type": "integer"
                            },
                            "sizeScale": "sqrt"
                        }
                    }
                ],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "xfyy9dm6": [
                                {
                                    "name": "Lat",
                                    "format": None
                                },
                                {
                                    "name": "Lng",
                                    "format": None
                                },
                                {
                                    "name": "PDS tracks",
                                    "format": None
                                }
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
                "bearing": 24,
                "dragRotate": True,
                "latitude": -8.919901184800565,
                "longitude": 125.51785785961475,
                "pitch": 50,
                "zoom": 7.948582452228619,
                "isSplit": False
            },
            "mapStyle": {
                "styleType": "dark",
                "topLayerGroups": {
                    "water": False,
                    "land": True,
                    "building": False,
                    "border": True,
                    "road": False,
                    "label": True
                },
                "visibleLayerGroups": {
                    "label": True,
                    "road": True,
                    "border": True,
                    "building": True,
                    "water": True,
                    "land": True,
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

    
    
