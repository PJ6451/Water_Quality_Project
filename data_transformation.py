from ca_open_data_api_2020 import *
import json

#load data
data = get_data()

#Single Sample WQO
dict = {
    "Enterococcus":110,
    "E. coli":320,
    "Coliform, Fecal":400,
    "Coliform, Total":10000,
}

for i in range(len(data)):
    #add upper limit
    row = data[i]
    type = row["DW_AnalyteName"]
    row["SingleSampleWQO"] = dict[type]
    
    #add exceedance
    if row["Result"] is not None:
        if float(row["Result"]) > row["SingleSampleWQO"]:
            row["Exceedance"] = "Does Exceed Limit"
        else:
            row["Exceedance"] = "Does Not Exceed Limit"
    else:
        row["Exceedance"] = "Not Detected/Recorded"
    
    #overwrite row to data
    data[i] = row

with open("safe_to_swim2020.json", "w") as final:
    json.dump(data, final)


