import requests, json, math, os, datetime, io, time, shutil
import pandas as pd
import numpy as np

class CovidManager:
    CONFIRMED_FULL_FILENAME = "covid_full_confirmed.csv"
    DEATHS_FULL_FILENAME = "covid_full_deaths.csv"

    def __init__(self, dataset_folder='dataset/', dataset_urls_csv='dataset/dataset_urls.csv', update=True, update_time=86400, backup=True, backup_folder='dataset/backup_covid/'):
        self.COLUMN_NAMES = ("dataset_label", "url")
        self.dataset_folder = dataset_folder
        self.dataset_urls = dataset_urls_csv
        self.update_time = update_time
        self.update = update
        self.backup = backup
        self.backup_folder = backup_folder

    def getFileName(self, data_label):
        return self.dataset_folder + data_label + ".csv"

    def backupDataset(self, df_urls):
        if self.backup:
            if not os.path.exists(self.backup_folder):
                os.mkdir(self.backup_folder)

            get_backup_filename = lambda data_label : self.backup_folder + data_label + ".csv"
            for index, row in df_urls.iterrows():
                data_label = row[self.COLUMN_NAMES[0]]
                filename = self.getFileName(data_label)
                if os.path.isfile(filename):
                    shutil.copyfile(filename, get_backup_filename(data_label))

        if os.path.isfile(self.dataset_folder + CovidManager.CONFIRMED_FULL_FILENAME):
            shutil.copyfile(self.dataset_folder + CovidManager.CONFIRMED_FULL_FILENAME, self.backup_folder + CovidManager.CONFIRMED_FULL_FILENAME)

        if os.path.isfile(self.dataset_folder + CovidManager.DEATHS_FULL_FILENAME):
            shutil.copyfile(self.dataset_folder + CovidManager.DEATHS_FULL_FILENAME, self.backup_folder + CovidManager.DEATHS_FULL_FILENAME)
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

            # Make columns of US dataset iso code table to be consistent with global covid dataset
            if 'us' in data_label or 'iso' in data_label:
                data_df = data_df.rename(columns={'Province_State' : 'Province/State',
                                                  'Country_Region' : 'Country/Region',
                                                  'Admin2' : 'County',
                                                  'Long_' : 'Long'})
            datasets[data_label] = data_df

            data_df.to_csv(self.getFileName(data_label), index=False, header=True)

        datasets['full'] = self.constructFullDataset(datasets)
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

        try:
            datasets['full'] = self.loadFullDataset()
        except:
            datasets['full'] = self.constructFullDataset(datasets)
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

        if os.path.isfile(self.dataset_folder + CovidManager.CONFIRMED_FULL_FILENAME):
            os.remove(self.dataset_folder + CovidManager.CONFIRMED_FULL_FILENAME)

        if os.path.isfile(self.dataset_folder + CovidManager.DEATHS_FULL_FILENAME):
            os.remove(self.dataset_folder + CovidManager.DEATHS_FULL_FILENAME)

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

    def loadFullDataset(self):
        full_dataset_dict = {'confirmed' : pd.read_csv(self.dataset_folder + CovidManager.CONFIRMED_FULL_FILENAME),
                             'deaths' : pd.read_csv(self.dataset_folder + CovidManager.DEATHS_FULL_FILENAME)}
        return full_dataset_dict

    def constructFullDataset(self, downloaded_df_dict):
        try:
            df_urls = pd.read_csv(self.dataset_urls, header=None, names=self.COLUMN_NAMES)
        except:
            print("There is no csv file at {} to specify the names of the files".format(self.dataset_urls))
            return None

        # Only have data for confirmed and deaths for US so full dataset will
        # only have those features
        full_dataset_dict = {'confirmed' : downloaded_df_dict['covid_confirmed'].copy(deep=True),
                             'deaths'    : downloaded_df_dict['covid_deaths'].copy(deep=True)}

        # Drop the single US data entry since we have a full datset for it
        # Create a column the county from the US dataset
        for dataset_name in full_dataset_dict:
            df = full_dataset_dict[dataset_name]

            df.insert(0, "County", [np.nan for ii in range(df.shape[0])], True)
            df = df.loc[df['Country/Region'] != 'US']
            full_dataset_dict[dataset_name] = df


        full_dataset_dict['confirmed'] = pd.concat([full_dataset_dict['confirmed'], downloaded_df_dict['covid_us_confirmed']], ignore_index=True, join="inner")
        full_dataset_dict['deaths'] = pd.concat([full_dataset_dict['deaths'], downloaded_df_dict['covid_us_deaths']], ignore_index=True, join="inner")

        full_dataset_dict['confirmed'].to_csv(self.dataset_folder + CovidManager.CONFIRMED_FULL_FILENAME, index=False, header=True)
        full_dataset_dict['deaths'].to_csv(self.dataset_folder + CovidManager.DEATHS_FULL_FILENAME, index=False, header=True)

        return full_dataset_dict

    def loadLatestBackup(self):
        print("Unable to download datasets from online. Loading backup datasets instead!")
        if not os.path.exists(self.backup):
            print("There are no backup datasets available!")
            return None

        try:
            df_urls = pd.read_csv(self.dataset_urls, header=None, names=self.COLUMN_NAMES)
        except:
            print("There is no csv file at {} to specify the names of the files".format(self.dataset_urls))
            return None

        get_backup_filename = lambda data_label : self.backup_folder + data_label + ".csv"
        failed = False
        self.clearDatasets()
        get_backup_data = lambda data_label : latest_backup[0] + "/" + data_label + ".csv"
        for index, row in df_urls.iterrows():
            data_label = row[self.COLUMN_NAMES[0]]
            backup_filename = get_backup_filename(data_label)
            filename = self.getFileName(data_label)

            if os.path.isfile(backup_filename):
                shutil.copyfile(backup_filename, filename)
            else:
                print("Unable to recover backup for " + filename)
                failed = True

        if failed: return None

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
            if not os.path.isfile(filename): return False

        if not os.path.isfile(self.dataset_folder + CovidManager.CONFIRMED_FULL_FILENAME): return False
        if not os.path.isfile(self.dataset_folder + CovidManager.DEATHS_FULL_FILENAME): return False

        return True


    def getDatasets(self):
        if self.update:
            if self.needsUpdating():
                try:
                    return self.downloadDataset()
                except:
                    return self.loadLatestBackup()
            else:
                return self.loadDatasets()
        else:
            if self.datasetsExist():
                return self.loadDatasets()
            else:
                try:
                    return self.downloadDataset()
                except:
                    return self.loadLatestBackup()

class AirportToLocation:

    def __init__(self, full_covid_df, airport_dataset_loc='dataset/airportDatabase.json', airport_location_dataset='dataset/airport_to_location.csv', iso_location_dataset='dataset/iso_table.csv'):
        """
        Need to parse in the full covid confirmed dataframe with US and Global data
        """
        self.covid_df = full_covid_df
        self.airport_dataset_fn = airport_dataset_loc
        self.airport_loc_fn = airport_location_dataset
        self.iso_df = pd.read_csv(iso_location_dataset)

    def generateNewAirportToLocationDataset(self):
        EARTH_RADIUS = 6371 # km

        def haversine_formula(lat0, long0, lat1, long1):
            lat0, long0, lat1, long1 = map(np.radians, [lat0, long0, lat1, long1])

            delta_lat = np.subtract(lat0, lat1)
            delta_long = np.subtract(long0, long1)

            a = np.sin(delta_lat/2)**2 + np.multiply(np.cos(lat0),np.cos(lat1)) * np.sin(delta_long/2)**2

            c = np.multiply(2,np.arctan2(np.sqrt(a), np.sqrt(1-a)))
            return np.multiply(c, EARTH_RADIUS)

        airport_dataset = {
            'codeIataAirport' : [],
            'County'          : [],
            'Province/State'  : [],
            'Country/Region'  : [],
            'LatAirport'      : [],
            'LongAirport'     : [],
        }

        try:
            with open(self.airport_dataset_fn, 'r') as json_file:
                airport_data = json.load(json_file)
        except IOError:
            print("Error occurred trying to load the Airport Dataset at {}".format(self.airport_dataset_fn))
            return None

        for airport_json in airport_data:
            # Get the country name from the the iso code table
            country_or_state_df = self.iso_df.loc[self.iso_df['iso2'] == airport_json['codeIso2Country']]
            if country_or_state_df.empty:
                del country_or_state_df
                continue

            # Each country has a unique iso2 code.
            # Covid datset lumps some countries inside of another one. So need to sort by state/province as well.
            country_name, state_name = country_or_state_df['Country/Region'].to_list()[0], country_or_state_df['Province/State'].to_list()[0]


            possible_locations_df = self.covid_df.loc[self.covid_df['Country/Region'] == country_name]

            # # If the country has multiple provinces and state_name isn't NaN then it is one of the weird edge cases.
            # if possible_locations_df.shape[0] > 1 and type(state_name) == str:
            #     del possible_locations_df
            #     possible_locations_df = self.covid_df.loc[self.covid_df['Province/State'] == state_name]

            distances_arr = haversine_formula(airport_json['latitudeAirport'], airport_json['longitudeAirport'], possible_locations_df['Lat'].to_numpy(), possible_locations_df['Long'].to_numpy())
            possible_locations_df['distance'] = distances_arr

            closest = possible_locations_df.sort_values('distance').iloc[0]

            airport_dataset['codeIataAirport'].append(airport_json['codeIataAirport'])
            airport_dataset['County'].append(closest['County'])
            airport_dataset['Province/State'].append(closest['Province/State'])
            airport_dataset['Country/Region'].append(closest['Country/Region'])
            airport_dataset['LatAirport'].append(airport_json['latitudeAirport'])
            airport_dataset['LongAirport'].append(airport_json['longitudeAirport'])

            del closest
            del possible_locations_df
            del country_or_state_df

        airport_df = pd.DataFrame(airport_dataset, columns=['codeIataAirport', 'County', 'Province/State', 'Country/Region', 'LatAirport', 'LongAirport'])

        try:
            airport_df.to_csv(self.airport_loc_fn, index=False, header=True)
        except:
            print("Error occurred trying to save Airport to Location Dataset to {}".format(self.airport_loc_fn))
        return airport_df

    def getDataset(self):
        if os.path.isfile(self.airport_loc_fn):
            return pd.read_csv(self.airport_loc_fn)
        else:
            return self.generateNewAirportToLocationDataset()

# NEEDS TO BE FIXED!
# class RouteManager:
#
#     def __init__(self, api_key="e32a49-08933a", airport_dataset = 'dataset/airport_to_location.csv', routes_dataset='dataset/routes.csv', num_of_threads=50):
#         try:
#             self.airport_df = pd.read_csv(airport_dataset)
#         except:
#             print("Error trying to read in the airport dataset at {}".format(airport_dataset))
#             return
#         self.api_key = api_key
#         self.api_call = "http://aviation-edge.com/v2/public/routes?key={api_key}&departureIata={depart_codeIata}&limit=30000"
#         self.routes_loc_fn = routes_dataset
#
#     def callAPI(self, iata_airport):
#         current_api_call = self.api_call.format(api_key = self.api_key, depart_codeIata=iata_airport)
#         response = requests.get(url=current_api_call)
#         json_data = response.json()
#         if type(json_data) == type({}):
#             return None
#         return json_data
#
#     def getStateCountryfromIata(self, iata_code):
#         airport = self.airport_df.loc[self.airport_df['codeIataAirport'] == iata_code]
#         for _index, row in airport.iterrows():
#             return row['Province/State'], row['Country/Region']
#         return None, None
#
#     def apiCallForRoutes(self):
#         """
#         WARNING! WE ONLY HAVE A LIMITED NUMBER OF CALLS SO AVOID CONSTANTLY CALLING THIS!
#         """
#         airport_route_dict = {
#             'Departure Province/State': [],
#             'Departure Country/Region': [],
#             'Arrival Province/State'  : [],
#             'Arrival Country/Region'  : [],
#         }
#
#         i = 0
#         for _index, row in self.airport_df.iterrows():
#             depart_iata_airport = row['codeIataAirport']
#             routes = self.callAPI(depart_iata_airport)
#             # Error handling json errors
#             if routes == None: continue
#             for route in routes:
#
#                 arrival_iata_airport = route['arrivalIata']
#                 state, country = self.getStateCountryfromIata(arrival_iata_airport)
#
#                 # If we do not have a record of that iata code
#                 if state == None and country == None: continue
#
#                 airport_route_dict['Departure Province/State'].append(row['Province/State'])
#                 airport_route_dict['Departure Country/Region'].append(row['Country/Region'])
#                 airport_route_dict['Arrival Province/State'].append(state)
#                 airport_route_dict['Arrival Country/Region'].append(country)
#
#             if i > 5: break
#             i += 1
#
#         route_df = pd.DataFrame(airport_route_dict, columns=['Departure Province/State',
#                                                              'Departure Country/Region',
#                                                              'Arrival Province/State',
#                                                              'Arrival Country/Region'])
#
#         try:
#             route_df.to_csv(self.routes_loc_fn, index=False, header=True)
#         except:
#             print("Error occurred trying to save Routes to Location Dataset to {}".format(self.routes_loc_fn))
