import os, subprocess
import pandas as pd
import numpy as np
import json
from datasetmanager import *

class CovidData:

    def __init__(self, routes_locations = 'dataset/airport_routes.csv',
                 border_closures_csv='dataset/border_closures.csv',
                 border_closures_json='dataset/border_closures.json',
                 eu_countries_csv='dataset/eu_countries.csv',
                 thread_num=20):
        """
        A wrapper class for organising all of the data for the COVID-19 project.

        parameters:
            routes_locations: the file location of the routes dataset mapping
                              locations in the COVID dataset to airports created
                              by the script download_route_dataset.py.

            thread_num:       the number of threads to use when running
                              download_route_dataset.py
        """
        self.border_closures_csv = border_closures_csv
        self.border_closures_json = border_closures_json
        self.eu_countries_csv = eu_countries_csv
        covid_manager = CovidManager()
        datasets = covid_manager.getDatasets()
        self.confirmed_df = datasets['full']['confirmed']
        self.deaths_df = datasets['full']['deaths']
        # Important Note! Recovered can only be used for global data!
        self.recovered_df = datasets['covid_recovered']
        airport_manager = AirportToLocation(datasets['full']['confirmed'])
        airport_manager.getDataset()

        if not os.path.isfile(routes_locations):
            subprocess.run(['python3', 'download_route_dataset.py', '-t', str(thread_num)], capture_output=True)

        self.routes_df = pd.read_csv(routes_locations)

    def createBorderDataset(self):
        border_closure_df = pd.read_csv(self.border_closures_csv, delimiter=':').fillna('none')
        eu_countries_str = '|'.join(pd.read_csv(self.eu_countries_csv)['Country'].tolist())

        def parse_border_list(list_str):
            list_str = list_str.replace("EU", eu_countries_str)
            return list_str.split("|")

        border_closures = {
            'country': {},
            'state': {}
        }


        join_location_name = lambda state, country: '{}:{}'.format(state, country)

        for _index, row in border_closure_df.iterrows():
            if row['Province/State'] == 'none':
                country_closures = border_closures['country'].get(row['Country/Region'], [])

                closure_dict = {
                    'date' : row['ClosureDate'],
                    'whitelist' : [],
                    'blacklist' : []
                }
                if not row['WhiteList'] == 'none':
                    closure_dict['whitelist'] = parse_border_list(row["WhiteList"])

                if not row['BlackList'] == 'none':
                    closure_dict['blacklist'] = parse_border_list(row["BlackList"])

                country_closures.append(closure_dict)

                border_closures['country'][row['Country/Region']] = country_closures
            else:
                location_name = join_location_name(row['Province/State'], row['Country/Region'])
                state_closures = border_closures['state'].get(location_name, [])

                closure_dict = {
                    'date' : row['ClosureDate'],
                    'whitelist' : [],
                    'blacklist' : []
                }
                if not row['WhiteList'] == 'none':
                    closure_dict['whitelist'] = parse_border_list(row["WhiteList"])

                if not row['BlackList'] == 'none':
                    closure_dict['blacklist'] = parse_border_list(row["BlackList"])

                state_closures.append(closure_dict)
                border_closures['state'][location_name] = state_closures

        for eu_country in eu_countries_str.split('|'):
            if eu_country not in border_closures and not eu_country == "Ireland":
                country_closures = border_closures['country'].get(eu_country, [])
                closure_dict = {
                    'date' : '3/17/20',
                    'whitelist' : parse_border_list("EU"),
                    'blacklist' : []
                }
                country_closures.append(closure_dict)
                border_closures['country'][eu_country] = country_closures


        with open(self.border_closures_json, 'w') as fp:
            json.dump(border_closures, fp)

    def loadBorderDataset(self):
        with open(self.border_closures_json, 'r') as fp:
            data = json.load(fp)
        return data


    def routesToWeightedEdges(self, bin_region_column, country):
        """
        Returns the number of routes going between locations based on
        bin_region_column and country parameters.

        parameters:
            bin_region_column: Is either 'county', 'state' or 'country'.
                               'county' : routes between locations should be
                                          based on county locations if possible.
                                          (Largest set)
                               'state'  : routes between locations should be
                                          based on state locations if possible.
                                          (Second largest set)
                               'country': routes between locations should be
                                          based on country locations.
                                          (Smallest set)

            country:           If None then all routes between countries are
                               returned. If not None then only looks at routes
                               within that country.

        returns:
            A dataframe specifying the number of routes between locations
            specified by the parameters.
        """
        new_routes_df = self.routes_df.copy(deep=True).fillna("none")

        if not country == None:
            new_routes_df = new_routes_df.loc[(new_routes_df['DepartCountry/Region'] == country) & (new_routes_df['ArrivalCountry/Region'] == country)]

        new_routes_df['NumberOfRoutes'] = 1
        agg_dict = {'NumberOfRoutes' : ['sum']}
        new_column = ['NumberOfRoutes']

        if bin_region_column == 'county':
            return new_routes_df.groupby([ 'DepartCounty',
                                           'DepartProvince/State',
                                           'DepartCountry/Region',
                                           'ArrivalCounty',
                                           'ArrivalProvince/State',
                                           'ArrivalCountry/Region'])['NumberOfRoutes'].sum().reset_index()
        if bin_region_column == 'state':
            return new_routes_df.groupby([ 'DepartProvince/State',
                                           'DepartCountry/Region',
                                           'ArrivalProvince/State',
                                           'ArrivalCountry/Region'])['NumberOfRoutes'].sum().reset_index()
        if bin_region_column == 'country':
            return new_routes_df.groupby([ 'DepartCountry/Region',
                                           'ArrivalCountry/Region'])['NumberOfRoutes'].sum().reset_index()

    def getData(self, bin_region_column='county', country=None, specific_date=None):
        """
        Gets the COVID data and routes inbetween locations based on the
        parameters of the function.

        You can only get the recovered dataset if bin_region_column='country'.

        parameters:
            bin_region_column: Is either 'county', 'state' or 'country'.
                               'county' : gets data with locations as counties,
                                          provinces/states and countries/regions
                                          (Largest set)
                               'state'  : gets data with locations as
                                          provinces/states and countries/regions
                                          (Second largest set)
                               'country': just gets data based on countries.
                                          (Smallest set)

            country:           If None then data from all countries will be used
                               Otherwise it will only return data for that
                               specific country.

            specific_date:     If None then returns data about all dates about
                               COVID.
                               Otherwise it will return the data for that
                               specific date.
                               If set to 'latest' then returns the data from the
                               latest date of recording.

        returns:
            Returns a dictionary stores the COVID data as dataframes based on
            the parameters and a dataframe storing the routes between locations
            in the COVID dataframes.
        """
        assert (bin_region_column == 'county') or (bin_region_column == 'state') or (bin_region_column == 'country'), "Invalid region parsed to bin_region_column! Needs to be county, state or country"

        data = {
            'confirmed' : self.confirmed_df.copy(deep=True).fillna("none"),
            'deaths'    : self.deaths_df.copy(deep=True).fillna("none")
        }

        if bin_region_column == 'country':
            data['recovered'] = self.recovered_df.copy(deep=True).fillna("none")

        if not country == None:
            for data_type in data:
                data[data_type] = data[data_type].loc[data[data_type]['Country/Region'] == country]

        dates = data['confirmed'].columns[5:].to_list()
        new_columns = ['Lat', 'Long'] + dates

        # Order is required so Lat and Long are before dates
        agg_dict = {}
        agg_dict['Lat'] = ['mean']
        agg_dict['Long'] = ['mean']

        if specific_date == None:
            for date in dates: agg_dict[date] = ['sum']
        elif not specific_date == 'latest':
            assert (specific_date in dates), "{} is not a valid date. Check the covid .csv files for what a valid dates look like!".format(specific_date)
            agg_dict[specific_date] = ['sum']
            new_columns = ['Lat', 'Long'] + [specific_date]
        else:
            latest_date = dates[-1]
            agg_dict[latest_date] = ['sum']
            new_columns = ['Lat', 'Long'] + [latest_date]


        for data_type in data:
            df = data[data_type]
            # County specific dataset is just the full COVID dataset
            if bin_region_column == 'county': continue
            if bin_region_column == 'state':
                new_df = df.groupby(['Province/State', 'Country/Region']).agg(agg_dict)
                new_df.columns = new_columns
                new_df = new_df.reset_index()
                data[data_type] = new_df
            if bin_region_column == 'country':
                new_df = df.groupby(['Country/Region']).agg(agg_dict)
                new_df.columns = new_columns
                new_df = new_df.reset_index()
                data[data_type] = new_df

        return data, self.routesToWeightedEdges(bin_region_column, country)
