import pandas as pd
import numpy as np

old_routes = pd.read_csv("dataset/airport_routes.csv")

old_routes = old_routes.replace("Channel Islands", np.nan)
old_routes = old_routes.replace("Isle of Man", np.nan)

old_routes.to_csv("airport_routes.csv")
