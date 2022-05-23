import pandas

def read_data():
    #read csv
    data = pandas.read_csv(r'C:\Users\16617\Downloads\safetoswim_2020-present_2022-05-23.csv')

    #keep only columns you need
    columns = [
        "StationCode",
        "Program",
        "Project",
        "ParentProject",
        "TargetLatitude",
        "TargetLongitude",
        "DW_AnalyteName",
        "Unit",
        "SampleDate",
        "MethodName",
        "RL",
        "Result",
        "ResultQualCode"
        ]

    data = data[columns]

    #save new csv
    data.to_csv('safetoswim_2020-present_2022-05-23_transformed.csv')

def data_transform(data: list,dic: dict) -> list:
    for i in range(len(data)):
        #add upper limit
        row = data[i]
        type = row["DW_AnalyteName"]
        row["SingleSampleWQO"] = dic[type]
        
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

    return data


#load data
data = read_data()

#Single Sample WQO
dic = {
    "Enterococcus":110,
    "E. coli":320,
    "Coliform, Fecal":400,
    "Coliform, Total":10000,
}

data = data_transform(data,dic)


