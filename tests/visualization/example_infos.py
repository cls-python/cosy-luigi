"""Template info dicts (as produced by ``repo_to_info``) used by the visualization tests."""

# examples/lot_sizing: data flow only between stage heads
LOT_SIZING = {
    "taxonomy": {
        "PredictDemandByLinearRegression": ["CoSyLuigiTask", "PredictDemand"],
        "GetCosts": ["CoSyLuigiTask"],
        "GetHistoricDemand": ["CoSyLuigiTask"],
        "OptimizeLotsByLeastUnitCost": ["CoSyLuigiTask", "OptimizeLots"],
        "OptimizeLotsBySilverMeal": ["CoSyLuigiTask", "OptimizeLots"],
        "PredictDemandByAverage": ["CoSyLuigiTask", "PredictDemand"],
        "OptimizeLotsByGroff": ["CoSyLuigiTask", "OptimizeLots"],
        "OptimizeLotsByWagnerWhitin": ["CoSyLuigiTask", "OptimizeLots"],
        "OptimizeLotsByPartPeriod": ["CoSyLuigiTask", "OptimizeLots"],
    },
    "requires": {
        "PredictDemand": ["GetHistoricDemand"],
        "OptimizeLots": ["GetCosts", "PredictDemand"],
    },
}

# examples/getting_started/visualization_example.py: data flow also from a stage head to an intermediate
# class and between concrete classes, including an overridden declaration
TITLES = {
    "taxonomy": {
        "WriteTemplateTask": ["CoSyLuigiTask"],
        "WriteMsTitleTemplateTask": ["CoSyLuigiTask", "WriteTitleTemplateTask"],
        "WriteMrTitleTemplateTask": ["CoSyLuigiTask", "WriteTitleTemplateTask"],
        "SubstituteNameByJohnDoeTask": ["CoSyLuigiTask", "SubstituteNameTask"],
        "SubstituteNameByMsJaneDoeTask": ["CoSyLuigiTask", "SubstituteNameTask", "SubstituteByNameAndTitleTask"],
        "SubstituteNameByMrJohnDoeTask": ["CoSyLuigiTask", "SubstituteNameTask", "SubstituteByNameAndTitleTask"],
    },
    "requires": {
        "SubstituteNameTask": ["WriteTemplateTask"],
        "SubstituteByNameAndTitleTask": ["WriteTitleTemplateTask"],
        "SubstituteNameByMsJaneDoeTask": ["WriteMsTitleTemplateTask"],
        "SubstituteNameByMrJohnDoeTask": ["WriteMrTitleTemplateTask"],
    },
}
