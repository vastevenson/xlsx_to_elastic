from elasticsearch import Elasticsearch
import csv
import pandas as pd
from dateutil.parser import parse

# make sure that you are using a recent version of the elasticsearch package!

xls_path = 'Active Work Order Products 3-26-2020 2-28-15 PM.xlsx'
csv_path = 'csv_transcript.csv'
index_name = 'vs_crm_wo_products_v0'
mapping = {
    "_doc": {
      "properties": {
        "(Do Not Modify) Modified On": {
          "type": "keyword"
        },
        "(Do Not Modify) Row Checksum": {
          "type": "keyword"
        },
        "(Do Not Modify) Work Order Product": {
          "type": "keyword"
        },
        "Agreement (Work Order) (Work Order)": {
          "type": "keyword"
        },
        "Agreement Type (Work Order) (Work Order)": {
          "type": "keyword"
        },
        "Closed By (Work Order) (Work Order)": {
          "type": "keyword"
        },
        "Closed On (Work Order) (Work Order)": {
          "type": "keyword"
        },
        "Created On": {
          "type": "date"
        },
        "Description": {
          "type": "keyword"
        },
        "Do Not Push to F&O (Work Order) (Work Order)": {
          "type": "keyword"
        },
        "ERP Transaction Document Number": {
          "type": "keyword"
        },
        "ERP Transfer Document Number": {
          "type": "keyword"
        },
        "Estimate Quantity": {
          "type": "double"
        },
        "Inventory Item": {
          "type": "keyword"
        },
        "Line Status": {
          "type": "keyword"
        },
        "On Hold Reason": {
          "type": "keyword"
        },
        "Owner (Work Order) (Work Order)": {
          "type": "keyword"
        },
        "Price List": {
          "type": "keyword"
        },
        "Product": {
          "type": "keyword"
        },
        "Product ID (Product) (Product)": {
          "type": "keyword"
        },
        "Product order status": {
          "type": "keyword"
        },
        "Quantity": {
          "type": "double"
        },
        "Service Account (Work Order) (Work Order)": {
          "type": "keyword"
        },
        "Shipment Arrival By": {
          "type": "keyword"
        },
        "Shipped On": {
          "type": "keyword"
        },
        "System Status (Work Order) (Work Order)": {
          "type": "keyword"
        },
        "Tracking Number": {
          "type": "keyword"
        },
        "Warehouse": {
          "type": "keyword"
        },
        "Work Order": {
          "type": "keyword"
        },
        "Work Order Title (Work Order) (Work Order)": {
          "type": "text"
        }
      }
    }
  }

def convert_xls_to_csv(xls_path, csv_path):
    xls = pd.read_excel(xls_path)
    xls.to_csv(csv_path)

def send_to_elastic(index_name, csv_path):
    HOSTS = ["172.16.1.81:9200", "172.16.1.125:9200", "172.16.1.126:9200"]
    es = Elasticsearch(hosts=HOSTS)

    headers = [] # declare empty list to save headers
    index = 0

    f = open(csv_path)
    csv_f = csv.reader(f)

    for row in csv_f:
      # the headers row does not contain a key, so if key does not exist, it as the header row
      if row[0]:
          # initialize an object dictionary to save the values from each row of the CSV into the keys (headers)
          obj = {}
          for i, val in enumerate(row):
              if(is_date(val)):
                  val = format_date(val)
              obj[headers[i]] = val
          es.index(index=index_name, doc_type="_doc", body=obj)
          print(obj)
          print()
      else:
          # save the headers row so we know what data
          headers = row
          headers[0]="key"

def recreate_index_with_mapping(index_name, mapping):
    HOSTS = ["172.16.1.81:9200", "172.16.1.125:9200", "172.16.1.126:9200"]
    es = Elasticsearch(hosts=HOSTS)
    # delete the old index
    es.indices.delete(index=index_name)
    # create a new index
    es.indices.create(index=index_name)
    # apply the mapping to the new index
    es.indices.put_mapping(index=index_name, doc_type="_doc", body=mapping)
    print(index_name + " index has been created with mapping applied in Elastic.")

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        try:
            parse(string, fuzzy=fuzzy)
        except:
            pass
        return True

    except ValueError:
        return False

def format_date(x):
    if "-" in x:
        x = x[0:x.find(' ')]
        # print(formatted_date)
    return x

# update an elastic index for the new dataset 
# recreate_index_with_mapping(index_name, mapping)

# convert the xlsx data into csv format
# convert_xls_to_csv(xls_path, csv_path)

# send csv data to elastic index
send_to_elastic(index_name, csv_path)