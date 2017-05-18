"""
This file takes two cmd line arguments at runtime:
    1) Path to list of change.org petition urls
    2) API key
in the following format:
    python get_data.py --url_list_path=path/to/list/of/urls --api_key=api/key

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

# Import for parsing cmd line arguments
import argparse

# Imports for obtaining and parsing API data
import requests
from time import sleep
import json

# Miscellaneous imports
from sys import exit
import pandas as pd


def is_json(data):
    """Method: Checks for whether passed data parameter is a json object."""
    try:
        json.loads(data)
    except ValueError:
        return False
    return True


class GetData(object):
    """Class for gathering petition data from the Change.org API."""
    def __init__(self):
        # Read path to url list and api key from cmd line args
        self.url_list_path, self.api_key = self.get_cmdline_args()

        # Read url list from path
        with open(self.url_list_path, "r+") as f:
            self.url_list = f.readlines()

    @staticmethod
    def get_cmdline_args():
        # Initialize arg parse object and define desired cmd line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--url_list_path", help="Path to list of change.org petition urls", type=str)
        parser.add_argument("--api_key", help="API key to make calls with", type=str)

        # Get path to list of petition urls and API key
        args = parser.parse_args()
        path, api = args.url_list_path, args.api_key

        # Check that all arguments are non-null
        if path and api:
            return path, api
        else:
            print "Missing some arguments. Please review usage..."
            print parser.print_help()
            exit()

    def output_filename(self):
        # Extract the appropriate file name from the path
        file_path = "/".join(self.url_list_path.split("/")[:-1])
        file_name = self.url_list_path.split("/")[-1].split("-")[1].split(".")[0] + "_data.csv"
        return "/".join([file_path, file_name])

    def get_response(self, input_url, first_try=True):
        # Make an api call and handle HTTP errors
        try:
            r = requests.request("GET", input_url.strip())
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as err:
            return "HTTP Error: {}".format(err.response.status_code)
        except requests.exceptions.ConnectionError as err:
            if first_try:
                sleep(5)
                self.get_response(input_url, first_try=False)
            else:
                return "Connection Error: {}".format(err.response.status_code)

    def get_petition_id(self, petition_url):
        # Make api call for petition id
        id_url = ("https://api.change.org/v1/petitions/"
                  "get_id?petition_url={0}&api_key={1}").format(petition_url.strip(),
                                                                self.api_key)
        response = self.get_response(id_url)

        try:
            id_json = json.loads(response.text)
            petition_id = id_json["petition_id"]
            return petition_id
        except AttributeError:
            return response

    def reasons_updates(self, petition_id, data="reasons"):
        # Check to make sure data flag is valid
        if data not in ["reasons", "updates"]:
            print ("Please choose valid data flag for reasons_updates method.\n"
                   "Valid choices: reasons/updates")
            exit(2)

        # Initialize array to store reasons/updates
        arr = []

        # Define initial API endpoint url for list of reasons/updates
        data_url = ("https://api.change.org/v1/petitions/{0}"
                    "/{1}?page_size=100&sort=time_asc&api_key={2}").format(petition_id,
                                                                           data,
                                                                           self.api_key)

        # Make initial API call
        response = self.get_response(data_url)

        # Start data collection
        try:
            # Get first batch of data
            data_json = json.loads(response.text)
            arr.extend(data_json[data])

            # Get total_pages value
            total_pages = int(data_json["total_pages"])

            # Make additional calls to collect data from remaining pages if they exist
            if total_pages > 1:
                for page in range(2, total_pages+1, 1):
                    # Form next API endpoint url
                    next_url = data_url + "&page={}".format(page)

                    # Make next call
                    response = self.get_response(next_url)

                    # Collect data
                    try:
                        data_json = json.loads(response.text)
                        arr.extend(data_json[data])
                    except AttributeError:
                        continue

            # Convert full reasons array into single json object
            data_json = json.dumps(arr)

            return data_json
        except AttributeError:
            return response

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

        try:
            data_json = json.loads(response.text)
            return data_json
        except AttributeError:
            return response


def main():
    # Initialize GetData object
    get_data = GetData()

    df = pd.DataFrame(columns=("id", "reasons", "updates", "data"))

    # Loop over urls
    for cnt, url in enumerate(get_data.url_list):
        # Get petition id
        _id = get_data.get_petition_id(url)

        if isinstance(_id, int):
            # Get reasons for signing petition
            reasons = get_data.reasons_updates(_id, data="reasons")

            # Get updates for petition
            updates = get_data.reasons_updates(_id, data="updates")

            # Get petition data
            data = get_data.petitions(_id)

            if is_json(reasons) and is_json(updates):
                # Organize all data into instance and add to data frame
                df.loc[cnt] = [str(_id), reasons, updates, data]
                print "Done with {}".format(_id)

                if (cnt + 1) % 25 == 0:
                    df.to_csv(get_data.output_filename(), index=False)
            else:
                continue
        else:
            continue

    df.to_csv(get_data.output_filename(), index=False)


if __name__ == "__main__":
    main()
