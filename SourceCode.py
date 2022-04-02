# pip install geopy
import requests
from bs4 import BeautifulSoup
from pprint import pprint
import time
import json
import regex as re
import numpy as np
import pandas as pd
from geopy.distance import distance
from pymongo import MongoClient

# Work flow for each city:
# 1. Find the geo coordinate using positionstack api;
# 2. Use the geo coordinate to collect future events nearby from ticketmaster api;
# 3. Collect local events from eventbrite in a given date range for current guests;
# 
# Finally, we store the events in 2 separate Mongo DB collections

# This function finds the geo coordinate of the hotel using positionstack api
def get_geo(address_text):
    search_url = "http://api.positionstack.com/v1/forward?access_key=[access_key]&query="
    url = search_url + address_text
    loc_page = requests.get(url).text
    time.sleep(3)
    loc_json = json.loads(loc_page)
    lat = loc_json['data'][0]['latitude']
    lon = loc_json['data'][0]['longitude']
    geo_coordinate = (lat, lon)
    return geo_coordinate


# These 2 functions use ticketmaster api to scrape future large events:
# tm_json_cleaning clean the json data;

# `latitude` and `longitude`: location of the hotel;
# `radius`: max distance between the address of the events and the hotel.
# `startdate`: events that happen after this date.(format: yyyy-mm-dd)
# `size`: how many top events we want to extract from the website

def tm_json_cleaning(Json_obj):
    # Get event names
    json_load = Json_obj.copy()
    Name = [json_load[i]['name'] for i in range(len(json_load))]  # 1
    # Get event type
    Classifications_json = [json_load[i]['classifications'] for i in range(len(json_load))]

    Segment_json = [Classifications_json[i][0]['segment'] for i in range(len(Classifications_json))]
    Segment = [Segment_json[i]['name'] for i in range(len(Segment_json))]  # 2

    # Get event date and time
    Date_json = [json_load[i]['dates'] for i in range(len(json_load))]

    StartDT_json = [Date_json[i]['start'] for i in range(len(Date_json))]
    DateTBA = [StartDT_json[i]['dateTBA'] for i in range(len(StartDT_json))]
    DateTBD = [StartDT_json[i]['dateTBD'] for i in range(len(StartDT_json))]
    TimeTBA = [StartDT_json[i]['timeTBA'] for i in range(len(StartDT_json))]
    NoSpecificTime = [StartDT_json[i]['noSpecificTime'] for i in range(len(StartDT_json))]

    LocalDate = []  # 3
    LocalTime = []  # 4
    for i in range(len(StartDT_json)):
        if not DateTBA[i] | DateTBD[i]:
            LocalDate.append(StartDT_json[i]['localDate'])
        else:
            LocalDate.append("TBD/TBA")
        if not TimeTBA[i]:
            if not NoSpecificTime[i]:
                LocalTime.append(StartDT_json[i]['localTime'])
            else:
                LocalTime.append("No Specific Time")
        else:
            LocalTime.append("TBA")
    # Get Distance
    Distance = [json_load[i]['distance'] for i in range(len(json_load))]  # 5
    # Event Location
    Embedded_json = [json_load[i]['_embedded'] for i in range(len(json_load))]
    Venues_json = [Embedded_json[i]['venues'] for i in range(len(Embedded_json))]
    Venue_name = [Venues_json[i][0]['name'] for i in range(len(Venues_json))]  # 6
    Address_json = [Venues_json[i][0]['address'] for i in range(len(Venues_json))]
    Address = []
    for i in range(len(Address_json)):
        try:
            Address.append(Address_json[i]['line1'])
        except:
            try:
                Address.append(Address_json[i]['line2'])
            except:
                Address.append("NULL")  # 7
    PostalCode = [Venues_json[i][0]['postalCode'] for i in range(len(Venues_json))]  # 8
    City_json = [Venues_json[i][0]['city'] for i in range(len(Venues_json))]
    City = [City_json[i]['name'] for i in range(len(City_json))]  # 9

    State_json = [Venues_json[i][0]['state'] for i in range(len(Venues_json))]
    State = [State_json[i]['stateCode'] for i in range(len(State_json))]  # 10

    # Event url
    Url = [json_load[i]['url'] for i in range(len(json_load))]  # 11
    columns = [
        'Name', 'Segment', 'LocalDate', 'LocalTime', 'Distance (mi)',
        'Venue_name', 'Address', 'PostalCode', 'City', 'State', 'Url'
    ]
    Event_df = pd.DataFrame(
        [
            Name, Segment, LocalDate, LocalTime, Distance,
            Venue_name, Address, PostalCode, City, State, Url
        ]
    ).transpose()
    Event_df.columns = columns
    Event_json = json.loads(Event_df.to_json(orient="records"))
    return Event_json


# Rates limit: 1000 events per day
def scrape_tm_events(latitude, longitude, radius, startdate, size):
    # Create url
    targeturl = "https://app.ticketmaster.com/discovery/v2/events.json?" \
                "includeTBA=no&&classificationId=-KZFzniwnSyZfZ7v7na"
    apikey = "Yourapikey"
    latlong = str(latitude) + "," + str(longitude)
    query_params = "&latlong=" + latlong + \
                   "&radius=" + str(radius) + \
                   "&startDateTime=" + startdate + "T00:00:00Z" + \
                   "&unit=miles&size=" + str(size) + \
                   "&apikey=" + apikey
    url = targeturl + query_params
    # Connect API and download json
    try:
        tm_page = requests.get(url)
        tm_soup = BeautifulSoup(tm_page.content, 'html.parser')
        json_load = json.loads(str(tm_soup))['_embedded']['events']
    except:
        print("could not find relevant events!")
    events_json = tm_json_cleaning(json_load)  # Use previously defined function to clean the json data
    return events_json


# Use Eventbrite to scrape for local events happen each week
# Event category we choose to provide to the guests:
# Travel & Outdoor
# Food & Drink
# Music
# Family & Education
# Fashion
# Film & Media
# Performing & Visual Arts

## date format: yyyy-mm-dd
def scrape_local_events(city, start_date, end_date):
    events_category = {
        "travel-and-outdoor": "Travel & Outdoor",
        "food-and-drink": "Food & Drink",
        "music": "Music",
        "family-and-education": "Family & Education",
        "arts": "Performing & Visual Arts"
    }
    address_list = {
        "ca--los-angeles": ["929 S Broadway, Los Angeles, CA 90015", "Los Angeles"],
        "wa--seattle": ["2423 1st Ave, Seattle, WA 98121", "Seattle"],
        "ny--new-york": ["20 W 29th St, New York, NY 10001", "New York"],
    }
    Event_json = []
    for category in events_category:
        print("  Scrape local {0} events".format(events_category[category]))
        request_url = "https://www.eventbrite.com/d/{0}/{1}--events/?" \
                      "start_date={2}&end_date={3}".format(city, category,
                                                           start_date, end_date)
        page = requests.get(request_url)
        time.sleep(5)
        soup = BeautifulSoup(page.content, 'html.parser')
        data_js = soup.find_all("script", {"type": "text/javascript"})[9]
        result = re.search(r'window.__SERVER_DATA__ = (.*);'
                           r'\n            \n            \n                    '
                           r'window.__REACT_QUERY_STATE__',
                           str(data_js)).group(1)
        events_info = json.loads(result)['search_data']['events']['results']

        # events address and time
        name = [e['name'] for e in events_info]
        category = [events_category[category]] * len(events_info)
        start_dt = [e['start_date'] + " " + e['start_time'] for e in events_info]
        end_dt = [e['end_date'] + " " + e['end_time'] for e in events_info]
        venue_name = [e['primary_venue']['name'] for e in events_info]
        address = [e['primary_venue']['address']['localized_multi_line_address_display'][0] for e in events_info]
        cities = [address_list[city][1]] * len(events_info)

        # calculate distance
        hotel_geo = get_geo(address_list[city][0])
        lon = [e['primary_venue']['address']['longitude'] for e in events_info]
        lat = [e['primary_venue']['address']['latitude'] for e in events_info]
        coords = [(lat[i], lon[i]) for i in range(len(lat))]
        distances = [round(distance(event_geo, hotel_geo).miles, 3) for event_geo in coords]

        # event intro and url
        brief_intro = [e['summary'] for e in events_info]
        url = [e['url'] for e in events_info]
        columns = [
            'City', 'Name', 'Category', 'StartDatetime', 'EndDatetime',
            'Distance (mi)', 'Venue_name', 'Address', 'Intro', 'Url'
        ]
        df = pd.DataFrame([cities, name, category, start_dt,
                           end_dt, distances, venue_name,
                           address, brief_intro, url
                           ]).transpose()
        df.columns = columns
        Event_json = Event_json + json.loads(df.to_json(orient="records"))
    return Event_json


# After running four defined functions, run the following code to scrape events for each city:

address_list = [
    ["929 S Broadway, Los Angeles, CA 90015", "ca--los-angeles"],
    ["2423 1st Ave, Seattle, WA 98121", "wa--seattle"],
    ["20 W 29th St, New York, NY 10001", "ny--new-york"]
]
future_events_json = []
local_events_json = []
for address in address_list:
    geo_point = get_geo(address[0])
    print("Scrape ticketmaster events for {0}".format(address[0].split(", ")[1]))
    future_events = scrape_tm_events(latitude=geo_point[0],
                                     longitude=geo_point[1],
                                     radius=10,
                                     startdate="2022-06-12",
                                     size=50)
    future_events_json += future_events
    local_events = scrape_local_events(address[1], "2022-03-14", "2022-03-20")
    local_events_json += local_events

# Print sample results
pprint(future_events_json[:3])

pprint(local_events_json[:3])

# Insert json objects into MongoDB

client = MongoClient(
    "mongodb+srv://dbYichao:JHOmeH7yoMvSOs1Z@cluster0.xdef3.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
)

db = client.events
eb_collection = db['eventbrite']
tm_collection = db['ticketmaster']

eb_collection.insert_many(local_events_json)
tm_collection.insert_many(future_events_json)
