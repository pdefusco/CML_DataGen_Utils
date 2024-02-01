import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime

def getStorage():
  #Extracting the correct URL from hive-site.xml
  tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
  root = tree.getroot()

  for prop in root.findall('property'):
      if prop.find('name').text == "hive.metastore.warehouse.dir":
          storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]

  print("The correct cloud storage is:{}".format(storage))

  return storage

getStorage()
