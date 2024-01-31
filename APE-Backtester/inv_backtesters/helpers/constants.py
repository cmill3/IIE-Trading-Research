CALL_STRATEGIES = ["IDXC","IDXC_1d","MA","MA_1d","GAIN_1d","GAIN","LOSERSC","LOSERSC_1d","VDIFFC","VDIFFC_1d","CDGAIN","CDGAIN_1d","CDMA","CDMA_1d","CDLOSEC","CDLOSEC_1d","CDBFC","CDBFC_1D",'CDHVC','CDHVC_1D']
PUT_STRATEGIES = ["IDXP","IDXP_1d","MAP","MAP_1d","GAINP_1d","GAINP","LOSERS","LOSERS_1d","VDIFFP","VDIFFP_1d","CDLOSE","CDLOSE_1d","CDMAP","CDMAP_1d","CDGAINP","CDGAINP_1d","CDBFP","CDBFP_1D",'CDHVP','CDHVP_1D']
ONED_STRATEGIES = ["IDXC_1d","IDXP_1d","MA_1d","MAP_1d","GAIN_1d","GAINP_1d","LOSERS_1d","LOSERSC_1d","VDIFFC_1d","VDIFFP_1d","CDGAIN_1d","CDMA_1d","CDLOSEC_1d","CDLOSE_1d","CDMAP_1d","CDGAINP_1d","CDBFC_1D","CDBFP_1D",'CDHVC_1D','CDHVP_1D']
THREED_STRATEGIES = ["IDXC","IDXP","MA","MAP","GAIN","GAINP","LOSERS","LOSERSC","VDIFFC","VDIFFP","CDGAIN","CDMA","CDLOSEC","CDLOSE","CDMAP","CDGAINP","CDBFC","CDBFP",'CDHVC','CDHVP']
ALGORITHM_CONFIG = {
    "GAIN_1D": {
        "target_label": "one_max",
        "target_value": .0175,
    },
    "GAIN": {
        "target_label": "three_max",
        "target_value": .0275,
    },
    "GAINP_1D": {
        "target_label": "one_min",
        "target_value": -.018,
    },
    "GAINP": {
        "target_label": "three_min",
        "target_value": -.028,
    },
     "LOSERSC_1D": {
        "target_label": "one_max",
        "target_value": .016,
    },
    "LOSERSC": {
        "target_label": "three_max",
        "target_value": .025,
    },
    "LOSERS_1D": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "LOSERS": {
        "target_label": "three_min",
        "target_value": -.02,
    },
    "vdiffC1d": {
        "target_label": "one_max",
        "target_value": .013,
    },
    "vdiffC": {
        "target_label": "three_max",
        "target_value": .028,
    },
    "vdiffP1d": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "vdiffP": {
        "target_label": "three_min",
        "target_value": -.028,
    },
     "MA_1D": {
        "target_label": "one_max",
        "target_value": .013,
    },
    "MA": {
        "target_label": "three_max",
        "target_value": .028,
    },
    "MAP_1D": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "MAP": {
        "target_label": "three_min",
        "target_value": -.028,
    },
    "idxC1d": {
        "target_label": "one_max",
        "target_value": .013,
    },
    "idxC": {
        "target_label": "three_max",
        "target_value": .028,
    },
    "idxP1d": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "idxP": {
        "target_label": "three_min",
        "target_value": -.028,
    },
}
