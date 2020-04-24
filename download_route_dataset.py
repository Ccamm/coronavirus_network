import requests, json, threading, queue, argparse, sys, random
import pandas as pd

# SECURITY RISK IF WE PUBLICALLY POST REPO!
# NEED TO CLEAR GITHUB COMMITS TOO SINCE IT WILL BE SAVED THERE AS WELL
API_KEY = "e32a49-08933a"
AIRPORT_DATASET = "dataset/airport_to_location.csv"
ROUTES_DATASET = "dataset/airport_routes.csv"
API_CALL = "http://aviation-edge.com/v2/public/routes?key={api_key}&departureIata={depart_codeIata}&limit=30000"
NUM_OF_THREADS = 8
NO_API_CALLS = False

# Used instead of requesting from API to see code functions normally if NO_API_CALLS == True
TEST_JSON = [{'arrivalIata': 'AAA'}]
TEST_RESET_CONN = 0.001
DEBUG_PRINT = False

airport_route_dict = {
    'DepartcodeIataAirport' : [],
    'DepartCounty'          : [],
    'DepartProvince/State'  : [],
    'DepartCountry/Region'  : [],
    'ArrivalcodeIataAirport': [],
    'ArrivalCounty'         : [],
    'ArrivalProvince/State' : [],
    'ArrivalCountry/Region' : [],
}

depart_call_q = queue.Queue()
push_to_dict_q = queue.Queue()

def parse_args():
    parser = argparse.ArgumentParser(description="Downloads the routes between locations from https://aviation-edge.com/")
    parser.add_argument("-k", "--api_key",
                        type=str,
                        help="the api key for the API calls to https://aviation-edge.com/",
                        default="e32a49-08933a")
    parser.add_argument("-a", "--airport_dataset",
                        type=str,
                        help="path to dataset mapping Iata codes to locations in the covid datasets.",
                        default="dataset/airport_to_location.csv")
    parser.add_argument("-r", "--route_dataset",
                        type=str,
                        help="where to save the route dataset when completed",
                        default="dataset/airport_routes.csv")
    parser.add_argument("-c", "--api_call",
                        type=str,
                        help="the REST API call to do that is formatted",
                        default="http://aviation-edge.com/v2/public/routes?key={api_key}&departureIata={depart_codeIata}&limit=30000")
    parser.add_argument("-t", "--thread_num",
                        type=int,
                        help="the number of threads to run (avoid making this too large and DOS the server)",
                        default=8)
    parser.add_argument("-n", "--no_api_calls",
                        type=bool,
                        help="tests the program and does not make any API requests, only prints what the API call would be",
                        default=False)
    parser.add_argument("-d", "--debug",
                        help="where to save the debug files specifying which iata codes have been sent to API. Set to None if you don't want to",
                        default=False)

    return parser.parse_args()

def get_info_from_iata(iata_code, airport_df):
    """
    NOTE: Not Thread Safe! Only call for the thread that puts data into dictionary
    """
    airport = airport_df.loc[airport_df['codeIataAirport'] == iata_code]
    for _index, row in airport.iterrows():
        return row['County'], row['Province/State'], row['Country/Region']
    return None, None, None

def call_api(iata_airport):
    current_api_call = API_CALL.format(api_key = API_KEY, depart_codeIata=iata_airport)

    if DEBUG_PRINT:
        print(iata_airport)

    if NO_API_CALLS:
        if random.random() < TEST_RESET_CONN:
            print("Test reset has been triggered for {}".format(iata_airport))
            return "reset"
        return TEST_JSON

    try:
        response = requests.get(url=current_api_call)
    except:
        print("Error connecting to server for Iata Code {}".format(iata_airport))
        return "reset"
    json_data = response.json()
    if type(json_data) == type({}):
        return None
    return json_data

def worker_api_call():
    while True:
        depart_iata_airport, depart_county, depart_state, depart_country = depart_call_q.get()
        if depart_iata_airport == None: break

        routes = call_api(depart_iata_airport)

        # If the connection is reset then put it back onto the queue
        if routes == "reset":
            depart_call_q.put((depart_iata_airport, depart_county, depart_state, depart_country))
            depart_call_q.task_done()
            continue
        # Error handling json errors
        if routes == None:
            depart_call_q.task_done()
            continue

        push_to_dict_q.put((depart_iata_airport, depart_county, depart_state, depart_country, routes))

        depart_call_q.task_done()

def worker_process():
    while True:
        depart_iata_airport, depart_county, depart_state, depart_country, routes = push_to_dict_q.get()
        if depart_country == None: break

        for route in routes:
            arrival_iata_airport = route['arrivalIata']

            arrival_county, arrival_state, arrival_country = get_info_from_iata(arrival_iata_airport, AIRPORT_DF)
            # If we do not have a record of that iata code ignore it
            if arrival_state == None and arrival_country == None: continue

            airport_route_dict['DepartcodeIataAirport'].append(depart_iata_airport)
            airport_route_dict['DepartCounty'].append(depart_county)
            airport_route_dict['DepartProvince/State'].append(depart_state)
            airport_route_dict['DepartCountry/Region'].append(depart_country)
            airport_route_dict['ArrivalcodeIataAirport'].append(arrival_iata_airport)
            airport_route_dict['ArrivalCounty'].append(arrival_county)
            airport_route_dict['ArrivalProvince/State'].append(arrival_state)
            airport_route_dict['ArrivalCountry/Region'].append(arrival_country)

        push_to_dict_q.task_done()

def main():
    if NUM_OF_THREADS < 2:
        print("{} is not enough threads to download the dataset".format(NUM_OF_THREADS))
        return
    threads = []
    print("Using the Aviation Edge API to create dataset for airplane routes!")

    for t_num in range(NUM_OF_THREADS - 1):
        thread = threading.Thread(target=worker_api_call, daemon=True)
        thread.start()
        threads.append(thread)

    processing_thread = threading.Thread(target=worker_process, daemon=True)
    processing_thread.start()

    for _index, row in AIRPORT_DF.iterrows():
        depart_iata_airport = row['codeIataAirport']
        depart_county = row['County']
        depart_state = row['Province/State']
        depart_country = row['Country/Region']

        depart_call_q.put((depart_iata_airport, depart_county, depart_state, depart_country))

    print("Completing all of the API calls...")

    depart_call_q.join()

    for thread in threads:
        depart_call_q.put((None, None, None, None))

    for thread in threads:
        thread.join()

    print("Completed making the API calls, just processing it to a dictionary")

    push_to_dict_q.join()

    push_to_dict_q.put((None, None, None, None, None))
    processing_thread.join()
    print("Finished processing data, now saving to file.")

    route_df = pd.DataFrame(airport_route_dict, columns=['DepartcodeIataAirport',
                                                         'DepartCounty',
                                                         'DepartProvince/State',
                                                         'DepartCountry/Region',
                                                         'ArrivalcodeIataAirport',
                                                         'ArrivalCounty',
                                                         'ArrivalProvince/State',
                                                         'ArrivalCountry/Region'])

    try:
        route_df.to_csv(ROUTES_DATASET, index=False, header=True)
    except:
        print("Error occurred trying to save Routes to Location Dataset to {}".format(ROUTES_DATASET))

    print("DONE! Dataset saved to {}".format(ROUTES_DATASET))


if __name__ == "__main__":
    args = parse_args()

    API_KEY = args.api_key
    AIRPORT_DATASET = args.airport_dataset
    ROUTES_DATASET = args.route_dataset
    API_CALL = args.api_call
    NUM_OF_THREADS = args.thread_num
    NO_API_CALLS = args.no_api_calls
    DEBUG_PRINT = args.debug
    AIRPORT_DF = pd.read_csv(AIRPORT_DATASET)

    main()
