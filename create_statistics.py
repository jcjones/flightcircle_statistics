#!/usr/bin/python3

import argparse
import csv
import json
import math
import pprint
import statistics
from datetime import datetime, timedelta
from collections import Counter, defaultdict, OrderedDict

config = {}


def is_weekend(start, end):
    if start.weekday() in [5, 6] or end.weekday() in [5, 6]:
        return True
    if end - start < timedelta(days=1):
        return False
    if end - start > timedelta(days=6):
        return True

    x = start
    while x < end:
        if x.weekday() in [5, 6]:
            return True
        x += timedelta(days=1)
    return False


def is_event_maintenance(event):
    return event["Type"] == "Maintenance"


def parse_datestamp(datestr):
    return datetime.strptime(datestr, "%Y-%m-%d %H:%M:%S")


def is_event_weekend(event):
    t_out = parse_datestamp(event["Start"])
    t_in = parse_datestamp(event["End"])
    return is_weekend(t_out, t_in)


def gather_metadata(event_list):
    # first_event = event_list[0]
    # last_event = event_list[len(event_list) - 1]
    first_event = None
    last_event = None

    for event in event_list:
        if is_event_maintenance(event) or not event["Tach Total"]:
            continue
        if not first_event or parse_datestamp(event["Start"]) < parse_datestamp(
            first_event["Start"]
        ):
            first_event = event
        if not last_event or parse_datestamp(event["End"]) > parse_datestamp(
            last_event["End"]
        ):
            last_event = event

    start_date = first_event["Start"]
    end_date = last_event["End"]

    t_out = parse_datestamp(start_date)
    t_in = parse_datestamp(end_date)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "length_days": (t_in - t_out).days,
        "num_events": len(event_list),
    }


def weekend_weekday_utilization(event_list):
    results = {"weekend": Counter(), "weekday": Counter()}
    for event in event_list:
        if is_event_weekend(event):
            results["weekend"]["total"] += 1
            results["weekend"][event["Aircraft"]] += 1
        else:
            results["weekday"]["total"] += 1
            results["weekday"][event["Aircraft"]] += 1
    return results


def airport_utilization(event_list):
    results = Counter()
    for event in event_list:
        results[event["Location"]] += 1
    return results


def airport_utilization_by_hours(event_list):
    results = Counter()
    for event in event_list:
        if event["Tach Total"]:
            results[event["Location"]] += float(event["Tach Total"])
    return results


def length_histogram(event_list):
    results = Counter()
    for event in event_list:
        if is_event_maintenance(event):
            continue

        t_out = parse_datestamp(event["Start"])
        t_in = parse_datestamp(event["End"])
        length_hours = int(math.ceil((t_in - t_out).total_seconds() / (60 * 60)))
        results[length_hours] += 1

    return OrderedDict(sorted(results.items(), key=lambda t: t[0]))


def days_between_usage(event_list):
    deltas_by_name = defaultdict(list)
    last_event_by_name = {}

    for event in event_list:
        aircraft_name = event["Aircraft"]
        if aircraft_name not in last_event_by_name:
            last_event_by_name[aircraft_name] = event
        else:
            if (
                parse_datestamp(event["Start"]).date()
                == parse_datestamp(last_event_by_name[aircraft_name]["End"]).date()
            ):
                continue

            # Don't count either side of a maintenance activity
            if not is_event_maintenance(event) and not is_event_maintenance(
                last_event_by_name[aircraft_name]
            ):
                delta_between = parse_datestamp(event["Start"]) - parse_datestamp(
                    last_event_by_name[aircraft_name]["End"]
                )
                deltas_by_name[aircraft_name].append(
                    abs(delta_between).total_seconds() / (60 * 60 * 24)
                )

            last_event_by_name[aircraft_name] = event

    return deltas_by_name


def usage_by_weekday(event_list):
    day_of_week_by_name = defaultdict(Counter)
    for event in event_list:
        if is_event_maintenance(event):
            continue

        x = parse_datestamp(event["Start"])
        while x < parse_datestamp(event["End"]):
            day_of_week_by_name[event["Aircraft"]][x.strftime("%A")] += 1
            x += timedelta(days=1)

    return day_of_week_by_name


def aircraft_available_by_airport_and_weekday(event_list, aircraft, airports):
    aircraft_per_airport, mod = divmod(len(aircraft), len(airports))
    if mod != 0:
        raise Exception("Uneven aircraft distribution!")

    # storage
    available_aircraft_by_airport_and_date = defaultdict(dict)
    # accumulators
    current_date = None
    aircraft_seen = []
    airport_usage = Counter()

    for event in event_list:
        if parse_datestamp(event["Start"]).date() != current_date:
            # Changing days, so let's record what we know (if we aren't at the beginning)
            if current_date is not None:
                # Note how many aircraft were not seen
                for airport in airports:
                    available_aircraft_by_airport_and_date[airport][current_date] = (
                        aircraft_per_airport - airport_usage[airport]
                    )

                # Initialize any gaps
                if parse_datestamp(event["Start"]).date() - current_date > timedelta(
                    days=1
                ):
                    x = current_date
                    while x < parse_datestamp(event["Start"]).date():
                        for airport in airports:
                            available_aircraft_by_airport_and_date[airport][x] = (
                                aircraft_per_airport
                            )
                        x += timedelta(days=1)

            current_date = parse_datestamp(event["Start"]).date()
            airport_usage.clear()
            aircraft_seen.clear()

        if event["Aircraft"] not in aircraft_seen:
            aircraft_seen.append(event["Aircraft"])
            airport_usage[event["Location"]] += 1

    airport_and_weekday_to_availability_list = {}
    for airport in airports:
        airport_and_weekday_to_availability_list[airport] = defaultdict(list)

    for airport, date_list in available_aircraft_by_airport_and_date.items():
        for date, count in date_list.items():
            airport_and_weekday_to_availability_list[airport][
                date.strftime("%A")
            ].append(count)

    airport_and_dow_to_mean_available_aircraft = defaultdict(dict)
    for airport, date_list in airport_and_weekday_to_availability_list.items():
        for dow, counts in date_list.items():
            airport_and_dow_to_mean_available_aircraft[airport][dow] = statistics.mean(
                counts
            )

    return airport_and_dow_to_mean_available_aircraft


def gather_aircraft(events):
    aircraft = []
    for evt in events:
        if evt["Aircraft"] not in aircraft:
            aircraft.append(evt["Aircraft"])
    return aircraft


def gather_locations(events):
    locations = []
    for evt in events:
        if evt["Location"] not in locations:
            locations.append(evt["Location"])
    return locations


def load_events(csvfile):
    reader = csv.DictReader(csvfile)

    rows = []

    for row in reader:
        rows.append(row)

    return rows


parser = argparse.ArgumentParser()
parser.add_argument("--json", help="output JSON to this file")
parser.add_argument("csv", help="Input CSV", type=argparse.FileType("r"))

args = parser.parse_args()

today = datetime.today()

events = load_events(args.csv)

aircraft = gather_aircraft(events)
locations = gather_locations(events)

dataset = {}
dataset["dataset_metadata"] = gather_metadata(events)
dataset["weekend_weekday_utilization"] = weekend_weekday_utilization(events)
dataset["airport_utilization"] = airport_utilization(events)
dataset["airport_utilization_by_hours"] = airport_utilization_by_hours(events)
dataset["length_of_reservation_by_hours"] = length_histogram(events)
dataset["days_between_usage_by_aircraft"] = days_between_usage(events)
dataset["usage_by_weekday"] = usage_by_weekday(events)
dataset["aircraft_available_by_airport_and_weekday"] = (
    aircraft_available_by_airport_and_weekday(events, aircraft, locations)
)


pprint.pprint(dataset)

if args.json:
    with open(args.json, "w") as outFile:
        json.dump(dataset, outFile)
