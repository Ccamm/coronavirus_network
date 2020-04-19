import requests, json, math, os, datetime, io, time
import pandas as pd

class CovidManager:
    def __init__(self, dataset_folder='dataset/', dataset_urls_csv='dataset/dataset_urls.csv', update_time=86400):
        self.COLUMN_NAMES = ("dataset_label", "url")
        self.dataset_folder = dataset_folder
        self.dataset_urls = dataset_urls_csv
        self.update_time = update_time

    def getFileName(self, data_label):
        return self.dataset_folder + "covid_" + data_label + ".csv"

    def downloadDataset(self):
        try:
            df_urls = pd.read_csv(self.dataset_urls, header=None, names=self.COLUMN_NAMES)
        except:
            print("There is no csv file at {} to specify where to download the dataset from".format(self.dataset_urls))
            return {}

        datasets = {}

        for index, row in df_urls.iterrows():
            data_label = row[self.COLUMN_NAMES[0]]
            url = row[self.COLUMN_NAMES[1]]
            dwnld = requests.get(url).content
            data_df = pd.read_csv(io.StringIO(dwnld.decode('utf-8').replace('\r', '')))
            datasets[data_label] = data_df

            data_df.to_csv(self.getFileName(data_label), index=False, header=True)

        return datasets

    def loadDatasets(self):
        try:
            df_urls = pd.read_csv(self.dataset_urls, header=None, names=self.COLUMN_NAMES)
        except:
            print("There is no csv file at {} to specify where to load the datasets from".format(self.dataset_urls))
            return {}

        datasets = {}
        for index, row in df_urls.iterrows():
            data_label = row[self.COLUMN_NAMES[0]]
            datasets[data_label] = pd.read_csv(self.getFileName(data_label))

        return datasets


    def clearDatasets(self):
        try:
            df_urls = pd.read_csv(self.dataset_urls, header=None, names=self.COLUMN_NAMES)
        except:
            print("There is no csv file at {} to specify the names of the files".format(self.dataset_urls))
            return

        for index, row in df_urls.iterrows():
            data_label = row[self.COLUMN_NAMES[0]]
            filename = self.getFileName(data_label)
            if os.path.isfile(filename):
                os.remove(filename)

    def needsUpdating(self):
        try:
            df_urls = pd.read_csv(self.dataset_urls, header=None, names=self.COLUMN_NAMES)
        except:
            print("There is no csv file at {} to specify the names of the files".format(self.dataset_urls))
            return False

        for index, row in df_urls.iterrows():
            data_label = row[self.COLUMN_NAMES[0]]
            filename = self.getFileName(data_label)
            if not os.path.isfile(filename):
                self.clearDatasets()
                return True
            modification_time = os.path.getmtime(filename)
            mod_time = datetime.datetime.fromtimestamp(modification_time)
            current_time = datetime.datetime.now()

            time_diff = time.mktime(current_time.timetuple()) - time.mktime(mod_time.timetuple())

            if time_diff > self.update_time:
                self.clearDatasets()
                return True

        return False

    def getDataSets(self):
        if self.needsUpdating():
            return self.downloadDataset()
        else:
            return self.loadDatasets()

class AirportToLocation:

    def __init__(self, airport_dataset_loc='dataset/airportDatabase.json', airport_location_dataset='dataset/airport_to_location.csv'):
        self.airport_dataset_fn = airport_dataset_loc
        self.airport_loc_fn = airport_location_dataset
        self.airport_df = None

    def generateNewAirportToLocationDataset(self, df):
        EARTH_RADIUS = 6371 # km

        def haversine_formula(latlong0, latlong1):

            lat0, long0 = math.radians(latlong0[0]), math.radians(latlong0[1])
            lat1, long1 = math.radians(latlong1[0]), math.radians(latlong1[1])

            delta_lat = lat0 - lat1
            delta_long = long0 - long1

            a = math.pow(math.sin(delta_lat/2),2) + math.cos(lat0) * math.cos(lat1) * math.pow(math.sin(delta_long/2),2)

            c = 2*math.atan2(math.sqrt(a),math.sqrt(1-a))
            return int(EARTH_RADIUS*c)

        try:
            with open(self.airport_dataset_fn, 'r') as json_file:
                airport_data = json.load(json_file)

                airport_dataset = {
                    'codeIataAirport' : [],
                    'codeIcaoAirport' : [],
                    'Country/Region'  : [],
                    'Province/State'  : [],
                }
                for airport_json in airport_data:
                    country_df = df.loc[df['Country/Region'] == airport_json['nameCountry']]
                    if country_df.empty:
                        country_df = df.loc[df['Province/State'] == airport_json['nameCountry']]
                        if country_df.empty:
                            continue

                    # Set to the approximate circumference of the world
                    shortest_distance = 2*math.pi*EARTH_RADIUS
                    shortest_country = "NONE"
                    shortest_state = "NONE"

                    for _n, row in country_df.iterrows():
                        dist = haversine_formula((airport_json['latitudeAirport'], airport_json['longitudeAirport']),
                                                 (row['Lat'], row['Long']))

                        if dist < shortest_distance:
                            shortest_distance = dist
                            shortest_country = row['Country/Region']
                            shortest_state = row['Province/State']

                    airport_dataset['codeIataAirport'].append(airport_json['codeIataAirport'])
                    airport_dataset['codeIcaoAirport'].append(airport_json['codeIcaoAirport'])
                    airport_dataset['Country/Region'].append(shortest_country)
                    airport_dataset['Province/State'].append(shortest_state)

                airport_df = pd.DataFrame(airport_dataset, columns=['codeIataAirport', 'codeIcaoAirport', 'Country/Region', 'Province/State'])

                try:
                    airport_df.to_csv(self.airport_loc_fn, index=False, header=True)
                except:
                    print("Error occurred trying to save Airport to Location Dataset to {}".format(self.airport_loc_fn))

                return airport_df
        except IOError:
            print("Error occurred trying to load the Airport Dataset at {}".format(self.airport_dataset_fn))

    def getDataset(self, dataframe):
        if os.path.isfile(self.airport_loc_fn):
            return pd.read_csv(self.airport_loc_fn)
        else:
            return self.generateNewAirportToLocationDataset(dataframe)
