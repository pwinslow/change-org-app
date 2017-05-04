"""
This file takes two cmd line arguments:
    1) Path to list of change.org petition urls
    2) API key

It then makes multiple API calls to change.org to perform the following tasks for each petition url:
    1) Obtain a unique petition id. This will used for all further API calls to collect petition data.
    2) Obtain petition reasons. Each person that signs the petition has an option to leave a reason for signing.
       This can help gauge the public response to the cause or the text of the petition.
    3) Obtain petition updates. The person who owns the petition has the option of providing updates on the petition
       at any point in time. This can tell us information about the time victory or loss occurred as well as gauge
       the progress of the petition over time.
    4) Obtain petition data and meta data. This is the meat of the data collection, it contains the petition text
       itself as well as any other data specified at the time of creation.

Once all the above data has been collected for a given petition, add a new row to a postgresSQL database.
"""
# TODO: Need a method to export petition data to single row of a csv.

# Import for parsing cmd line arguments
import argparse

# Imports for obtaining and parsing API data
import requests
from time import sleep
import json

# Miscellaneous imports
from sys import exit


class GetData(object):
    def __init__(self):
        # Read path to url list and api key from cmd line args
        list_path, self.api_key = self.get_cmdline_args()

        # Read url list from path
        with open(list_path, "r+") as f:
            self.url_list = f.readlines()

    @staticmethod
    def get_cmdline_args():
        # Initialize argparse object and define desired cmd line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--list_path", help="Path to list of change.org petition urls", type=str)
        parser.add_argument("--api_key", help="API key to make calls with", type=str)

        # Get path to list of petition urls and API key
        args = parser.parse_args()
        path, api = args.list_path, args.api_key

        # Check that all arguments are non-null
        if path and api:
            return path, api
        else:
            print "Missing some arguments. Please review usage..."
            print parser.print_help()
            exit()

    @staticmethod
    def get_response(input_url):
        # Make an api call
        try:
            r = requests.request("GET", input_url.strip())

            # If status code is 429, sleep for 2 seconds and try again
            while r.status_code == 429:
                sleep(2)
                r = requests.request("GET", input_url.strip())

            # If error arises, return error code
            if r.status_code != 200:
                return "Error: status code {}".format(r.status_code)

            return r
        except requests.exceptions.ConnectionError as e:
            print "Failed to establish connection. This normally occurs when there is an error in the input url."
            print "Official error message will print below...\n"
            print e
            exit(2)

    def get_petition_id(self, petition_url):
        # Make api call for petition id
        id_url = ("https://api.change.org/v1/petitions/"
                  "get_id?petition_url={0}&api_key={1}").format(petition_url.strip(),
                                                                self.api_key)
        response = self.get_response(id_url)

        # Extract petition id from json response
        if (type(response) == str) and (response.startswith("Error")):
            print response
            exit(2)
        else:
            id_json = json.loads(response.text)
            petition_id = id_json["petition_id"]

            return petition_id

    def reasons_updates(self, petition_id, data="reasons"):
        # Check to make sure data is valid
        if data not in ["reasons", "updates"]:
            print ("Please choose valid data flag for reasons_updates method.\n"
                   "Valid choices: reasons/updates")
            exit(2)

        # Initialize array to store reasons/updates
        arr = []

        # Define initial url for api call for list of reasons/updates
        data_url = ("https://api.change.org/v1/petitions/{0}"
                    "/{1}?page_size=100&sort=time_asc&api_key={2}").format(petition_id,
                                                                           data,
                                                                           self.api_key)

        # Continue filling arr until next_page_endpoint is None
        while data_url:
            # Make api call
            response = self.get_response(data_url)

            # Extract reasons from json response and reset data_url
            if (type(response) == str) and (response.startswith("Error")):
                print response
                exit(2)
            else:
                data_json = json.loads(response.text)
                arr.extend(data_json[data])
                data_url = data_json["next_page_endpoint"]
                if data_url:
                    data_url += "&sort=time_desc&api_key={0}".format(self.api_key)

        # Convert full reasons array into single json object
        data_json = json.dumps(arr)

        return data_json

    def petitions(self, petition_id):
        # Specify fields to collect from petition data
        fields = ",".join(["title",
                           "status",
                           "targets",
                           "overview",
                           "letter_body",
                           "signature_count",
                           "category",
                           "goal",
                           "created_at",
                           "end_at",
                           "creator_name",
                           "creator_url",
                           "organization_name",
                           "organization_url"])

        # Define initial url for api call for petition data
        data_url = ("https://api.change.org/v1/petitions/{0}"
                    "?fields={1}&api_key={2}").format(petition_id,
                                                      fields,
                                                      self.api_key)

        # Make api call
        response = self.get_response(data_url)

        # Extract reasons from json response and reset data_url
        if (type(response) == str) and (response.startswith("Error")):
            print response
            exit(2)
        else:
            data_json = json.loads(response.text)

            return data_json


if __name__ == "__main__":
    get_data = GetData()
    url = get_data.url_list[0]
    p_id = get_data.get_petition_id(url)

    # result = get_data.reasons_updates(p_id, data="reasons")
    result = get_data.petitions(p_id)

    print result["title"]
