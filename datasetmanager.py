import requests, json, math, os, datetime, io, time, shutil, threading, queue
import pandas as pd

class CovidManager:
    def __init__(self, dataset_folder='dataset/', dataset_urls_csv='dataset/dataset_urls.csv', update=True, update_time=86400, backup=True, backup_folder='dataset/backup_covid/'):
        self.COLUMN_NAMES = ("dataset_label", "url")
        self.dataset_folder = dataset_folder
        self.dataset_urls = dataset_urls_csv
        self.update_time = update_time
        self.update = update
        self.backup = backup
        self.backup_folder = backup_folder

    def getFileName(self, data_label):
        return self.dataset_folder + "covid_" + data_label + ".csv"

    def backupDataset(self, df_urls):
        if self.backup:
            if not os.path.exists(self.backup_folder):
                os.mkdir(self.backup_folder)

            current_backup = self.backup_folder + datetime.datetime.now().strftime("%d%m%Y_%H%M%S") + "/"
            os.mkdir(current_backup)
            get_backup_filename = lambda data_label : current_backup + "covid_" + data_label + ".csv"
            for index, row in df_urls.iterrows():
                data_label = row[self.COLUMN_NAMES[0]]
                filename = self.getFileName(data_label)
                if os.path.isfile(filename):
                    shutil.copyfile(filename, get_backup_filename(data_label))

        self.clearDatasets()

    def downloadDataset(self):
        try:
            df_urls = pd.read_csv(self.dataset_urls, header=None, names=self.COLUMN_NAMES)
        except:
            print("There is no csv file at {} to specify where to download the dataset from".format(self.dataset_urls))
            return {}

        self.backupDataset(df_urls)
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
                return True
            modification_time = os.path.getmtime(filename)
            mod_time = datetime.datetime.fromtimestamp(modification_time)
            current_time = datetime.datetime.now()

            time_diff = time.mktime(current_time.timetuple()) - time.mktime(mod_time.timetuple())

            if time_diff > self.update_time:
                return True

        return False

    def loadLatestBackup(self):
        if not os.path.exists(self.backup):
            return None

        try:
            df_urls = pd.read_csv(self.dataset_urls, header=None, names=self.COLUMN_NAMES)
        except:
            print("There is no csv file at {} to specify the names of the files".format(self.dataset_urls))
            return None

        backup_list = [backup[0] for backup in os.walk(self.backup_folder)]

        try:
            latest_backup = (backup_list[1], os.path.getmtime(backup_list[1]))
        except:
            print("There are no backups...")
            return None

        for ii in range(2, len(backup_list)):
            folder_name =  backup_list[ii]
            mod_time = os.path.getmtime(folder_name)
            if mod_time > latest_backup[1]:
                latest_backup = (folder_name, mod_time)

        self.clearDatasets()
        get_backup_data = lambda data_label : latest_backup[0] + "/" + "covid_" + data_label + ".csv"
        for index, row in df_urls.iterrows():
            data_label = row[self.COLUMN_NAMES[0]]
            backup_filename = get_backup_data(data_label)
            filename = self.getFileName(data_label)
            shutil.copyfile(backup_filename, filename)

        return self.loadDatasets()

    def datasetsExist(self):
        try:
            df_urls = pd.read_csv(self.dataset_urls, header=None, names=self.COLUMN_NAMES)
        except:
            print("There is no csv file at {} to specify the names of the files".format(self.dataset_urls))
            return False

        for index, row in df_urls.iterrows():
            data_label = row[self.COLUMN_NAMES[0]]
            filename = self.getFileName(data_label)
            if not os.path.isfile(filename):
                return False
        return True


    def getDataSets(self):
        if self.update:
            if self.needsUpdating():
                try:
                    return self.downloadDataset()
                except:
                    self.loadLatestBackup()
            else:
                return self.loadDatasets()
        else:
            if self.datasetsExist():
                return self.loadDatasets()
            else:
                try:
                    return self.downloadDataset()
                except:
                    self.loadLatestBackup()

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

class RouteManager:

    def __init__(self, api_key="e32a49-08933a", airport_dataset = 'dataset/airport_to_location.csv', routes_dataset='dataset/routes.csv', num_of_threads=50):
        try:
            self.airport_df = pd.read_csv(airport_dataset)
        except:
            print("Error trying to read in the airport dataset at {}".format(airport_dataset))
            return
        self.api_key = api_key
        self.api_call = "http://aviation-edge.com/v2/public/routes?key={api_key}&departureIata={depart_codeIata}&limit=30000"
        self.routes_loc_fn = routes_dataset

    def callAPI(self, iata_airport):
        current_api_call = self.api_call.format(api_key = self.api_key, depart_codeIata=iata_airport)
        response = requests.get(url=current_api_call)
        json_data = response.json()
        if type(json_data) == type({}):
            return None
        return json_data

    def getStateCountryfromIata(self, iata_code):
        airport = self.airport_df.loc[self.airport_df['codeIataAirport'] == iata_code]
        for _index, row in airport.iterrows():
            return row['Province/State'], row['Country/Region']
        return None, None

    def apiCallForRoutes(self):
        """
        WARNING! WE ONLY HAVE A LIMITED NUMBER OF CALLS SO AVOID CONSTANTLY CALLING THIS!
        """
        airport_route_dict = {
            'Departure Province/State': [],
            'Departure Country/Region': [],
            'Arrival Province/State'  : [],
            'Arrival Country/Region'  : [],
        }

        i = 0
        for _index, row in self.airport_df.iterrows():
            depart_iata_airport = row['codeIataAirport']
            routes = self.callAPI(depart_iata_airport)
            # Error handling json errors
            if routes == None: continue
            for route in routes:

                arrival_iata_airport = route['arrivalIata']
                state, country = self.getStateCountryfromIata(arrival_iata_airport)

                # If we do not have a record of that iata code
                if state == None and country == None: continue

                airport_route_dict['Departure Province/State'].append(row['Province/State'])
                airport_route_dict['Departure Country/Region'].append(row['Country/Region'])
                airport_route_dict['Arrival Province/State'].append(state)
                airport_route_dict['Arrival Country/Region'].append(country)

            if i > 5: break
            i += 1

        route_df = pd.DataFrame(airport_route_dict, columns=['Departure Province/State',
                                                             'Departure Country/Region',
                                                             'Arrival Province/State',
                                                             'Arrival Country/Region'])

        try:
            route_df.to_csv(self.routes_loc_fn, index=False, header=True)
        except:
            print("Error occurred trying to save Routes to Location Dataset to {}".format(self.routes_loc_fn))
