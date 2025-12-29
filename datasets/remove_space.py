import pandas as pd
import numpy as np

for data_name in ["airports", "hospital", "inspection", "ncvoter", "adults", "aminer"]:
    data_path = data_name + "_sample/" + data_name + "_sample.csv"
    data = pd.read_csv(data_path)

    revised_attrs = None
    if data_name == "airports":
        revised_attrs = ["id", "elevation_ft"]
    elif data_name == "ncvoter":
        revised_attrs = ["county_id", "id"]
    elif data_name == "hospital":
        revised_attrs = ["ZIP_Code", "Phone_Number"]
    elif "tax" in data_name:
        revised_attrs = ["areacode", "phone", "zip", "salary", "singleexemp", "marriedexemp", "childexemp"]
    elif "aminer_merged_categorical" in data_name:
        revised_attrs = ["author2paper_id", "author_id", "paper_id", "author_id"]
    elif "aminer" in data_name:
        revised_attrs = ["author2paper_id", "author_id", "paper_id", "author_position"] + ["author_id", "published_papers", "citations", "h_index"] + ["paper_id", "year"]
    elif "adults_categorical" in data_name:
        revised_attrs = ["age", "education_num", "capital_gain", "capital_loss", "hours_per_week"]
    elif "adults" in data_name:
        revised_attrs = ["age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week"]
    
    if revised_attrs is not None:
        for attr in revised_attrs:
            data[attr] = data[attr].astype(str).str.replace('\.0$', '').replace("nan", np.nan)

    for attr in data.columns:
        data[attr] = data[attr].astype(str).str.replace(' ', '##').replace("nan", np.nan)
    
    data.to_csv(data_name + "_sample/" + data_name + "_sample_removeSpace.csv", index=False)
