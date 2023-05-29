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
                        "id": "97csh2j",
                        "type": "hexagon",
                        "config": {
                            "dataId": "dat_tracks",
                            "label": "PDS data",
                            "color": [
                                255,
                                203,
                                153
                            ],
                            "highlightColor": [
                                252,
                                242,
                                26,
                                255
                            ],
                            "columns": {
                                "lat": "Lat",
                                "lng": "Lng"
                            },
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 1,
                                "worldUnitSize": 0.8,
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
                                "sizeRange": [
                                    0,
                                    500
                                ],
                                "percentile": [
                                    0,
                                    100
                                ],
                                "elevationPercentile": [
                                    0,
                                    100
                                ],
                                "elevationScale": 43.5,
                                "enableElevationZoomFactor": True,
                                "colorAggregation": "average",
                                "sizeAggregation": "sum",
                                "enable3d": True
                            },
                            "hidden": False,
                            "textLabel": [
                                {
                                    "field": None,
                                    "color": [
                                        255,
                                        255,
                                        255
                                    ],
                                    "size": 18,
                                    "offset": [
                                        0,
                                        0
                                    ],
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
                            "zzzm5iti": [
                                {
                                    "name": "Lng",
                                    "format": None
                                },
                                {
                                    "name": "Lat",
                                    "format": None
                                },
                                {
                                    "name": "GPS tracks",
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
                "bearing": 8.248859637225436,
                "dragRotate": True,
                "latitude": -8.802598271870869,
                "longitude": 125.42711793021567,
                "pitch": 37.5812678806278,
                "zoom": 8.091071712285888,
                "isSplit": False
            },
            "mapStyle": {
                "styleType": "om4051n",
                "topLayerGroups": {
                    "label": True,
                    "border": True,
                    "water": False
                },
                "visibleLayerGroups": {
                    "label": True,
                    "road": False,
                    "border": True,
                    "building": False,
                    "water": True
                },
                "threeDBuildingColor": [
                    194.6103322548211,
                    191.81688250953655,
                    185.2988331038727
                ],
                "mapStyles": {
                    "om4051n": {
                        "accessToken": None,
                        "custom": True,
                        "icon": "https://api.mapbox.com/styles/v1/langbart/cli8oua4m002a01pg17wt6vqa/static/-122.3391,37.7922,9,0,0/400x300?access_token=pk.eyJ1IjoidWNmLW1hcGJveCIsImEiOiJja2tyMjNhcWIwc29sMnVzMThoZ3djNXhzIn0._hfBNwCD7pCU7RAMOq6vUQ&logo=False&attribution=False",
                        "id": "om4051n",
                        "label": "Untitled",
                        "url": "mapbox://styles/langbart/cli8oua4m002a01pg17wt6vqa"
                    }
                }
            }
        }
    }

    kpmap = KeplerGl(height=400)
    kpmap.add_data(data=df, name='dat_tracks')
    kpmap.config = config
    kpmap = KeplerGl(height=400, data={'dat_tracks': df}, config=config)
    kpmap.save_to_html(data={'dat_tracks': df}, config=config, file_name='kepler_pds_map.html', read_only = "True")

    
    
