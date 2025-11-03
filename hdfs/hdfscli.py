from dataclasses import dataclass
import json
import os
from os.path import exists
from traceback import print_exception
import requests
import sys


@dataclass
class State:
    host: str
    port: int
    user: str
    hpath: list[str]
    lpath: list[str]

    def mkdir(self, path: str):
        resp = requests.put(self._makeurl(path, "MKDIRS"))
        print(resp.json())

    def put(self, path: str):
        filename = path.split("/")[-1]
        url = self._makeurl(filename, "CREATE") + "&noredirect=true"
        resp = requests.put(url)

        loc = str(resp.json()["Location"])
        url = "http://localhost:" + ":".join(loc.split(":")[2:])
        resp = requests.put(url, data=open(path, "rb").read())
        print("OMG! SUCCCESS!")
        
    def get(self, path:str):
        url = self._makeurl(path, "OPEN") + "&noredirect=true"
        resp = requests.get(url)

        loc = str(resp.json()["Location"])
        url = "http://localhost:" + ":".join(loc.split(":")[2:])
        resp = requests.get(url)
        filename = path.split("/")[-1]
        inc = ""
        while exists(filename + str(inc)):
            if inc == "":
                inc = 1
            else:
                inc+= 1

        with open(filename+str(inc), mode="w") as f:
            f.write(resp.content.decode())
            
        print("OMG! SUCCCESS!")

    def append(self, local_path: str, hdfs_path):
        url = self._makeurl(hdfs_path, "APPEND") + "&noredirect=true"
        resp = requests.post(url)

        loc = str(resp.json()["Location"])
        url = "http://localhost:" + ":".join(loc.split(":")[2:])
        resp = requests.post(url, data=open(local_path).read())
        print("OMG! SUCCCESS!")

    def delete(self, path: str):
        resp = requests.delete(self._makeurl(path, "DELETE") + "&recursive=true")
        print(resp.json())

    def ls(self):
        resp = requests.get(self._makeurl("/".join(self.hpath), "LISTSTATUS"))

        for  file in resp.json()["FileStatuses"]["FileStatus"]:
            print(f"{file["type"]} {file["length"]} {file["permission"]} {file["owner"]}:{file["group"]} {file["pathSuffix"]}")

    def cd(self, path: str):
        url = ""
        if path == "..": 
            hpath = self.hpath[:-1]
            url = self._makeurl("/".join(hpath), "LISTSTATUS")        
        else:
            url = self._makeurl("/".join(self.hpath) + "/" + path, "LISTSTATUS")

        resp = requests.get(url)
        if resp.status_code == 404:
            print("Directory not found!")
        elif len(resp.json()["FileStatuses"]["FileStatus"]) > 0 and resp.json()["FileStatuses"]["FileStatus"][0]["pathSuffix"] == "":
            print("Cannot cd into file!")
        else:
            if path =="..":
                self.hpath = self.hpath[:-1]
            else:
                self.hpath.append(path)
        print(self.hpath)

    def lls(self):
        print(os.listdir(os.curdir))

    def lcd(self, path: str):
        os.chdir(path)
        self.lls()

    def _makeurl(self, path: str, op: str) -> str:
        return f"http://{self.host}:{self.port}/webhdfs/v1/{path}?user.name={self.user}&op={op}"

    def help(self):
        print('''
    ls <PATH> - list files
    help      - show this help message
    ''')

def main():
    args = sys.argv
    state: State
    try: 
        state = State(host=args[1], port=int(args[2]), user=args[3], hpath=[], lpath=[])
    except Exception as e:
        print(f"Wrong arguments: {e}")
        exit(1)

    while True:
        print(">", end=" ")
        command = input().split()
        try:
            match command[0]:
                case "ls":
                    state.ls()
                case "mkdir":
                    state.mkdir(command[1])
                case "put":
                    state.put(command[1])
                case "get":
                    state.get(command[1])
                case "append":
                    state.append(command[1], command[2])
                case "delete":
                    state.delete(command[1])
                case "cd":
                    state.cd(command[1])
                case "lls":
                    state.lls()
                case "lcd":
                    state.lcd(command[1])
                case _:
                    state.help()

        except Exception as e:
            print_exception(e)

if __name__ == "__main__":
    main()
