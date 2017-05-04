"""
This file sets up multiple jobs to run on a PBS cluster. Each job collects data on a list of petitions.
"""

# Miscellaneous imports
from sys import exit
from csv import reader
from os import getcwd, listdir, remove, system
from os.path import join, isfile, isdir


class Runner(object):
    def __init__(self):
        self.key_path = join(getcwd(), "API_key_list")
        self.data_path = join(getcwd(), "data/xml_data")
        self.key_list = self.get_keys(self.key_path)
        self.file_list = self.get_files(self.data_path)
        self.run_names = [name.split('-')[1].split('.')[0] for name in self.file_list]

    @staticmethod
    def get_keys(key_path):
        # If API key file exists, read it and extract list of keys
        if isfile(key_path):
            key_list = []
            with open(key_path, "rb") as f:
                key_csv = reader(f)
                for row in key_csv:
                    key = row[3].strip()
                    if key != "api_key":
                        key_list.append(key)
            return key_list
        else:
            print "API key file is not in {}.".format(key_path)
            exit(2)

    @staticmethod
    def get_files(data_path):
        # If data folder exists, list files in it and extract filenames for all .dat files
        if isdir(data_path):
            file_list = []
            files = listdir(data_path)
            for file_name in files:
                if file_name.endswith("dat"):
                    file_list.append(file_name)
            return file_list
        else:
            print "Data files are not in {}.".format(data_path)

    def run(self):
        # For each data file, write script to collect petition data using GetData methods running on cluster
        for run_name, file_name, api_key in zip(self.run_names, self.file_list, self.key_list):
            # Check if run script exists, if so then rm it
            script_path = join(getcwd(), "script.sh")
            if isfile(script_path):
                remove(script_path)

            # Create submission script
            run_cmd = "python {0}/get_data.py --list_path={1} --api_key={2}\n".format(getcwd(),
                                                                                      join(self.data_path, file_name),
                                                                                      api_key)
            with open(script_path, "w") as f:
                f.write("#/bin/sh\n")
                f.write("#PBS -N Scan-{}\n".format(run_name))
                f.write(run_cmd)

            # Run submission script
            system("chmod +x script.sh")
            system("./script.sh")

if __name__ == "__main__":
    runner = Runner()
    runner.run()
