
from sqlalchemy.sql import func,cast,desc,asc
from models import Activity,BusinessHours,Base,Timezone
import datetime
from datetime import timedelta
import pytz
import csv
import os



def convert_to_local(timeobj,time_zone):

    time_zone = pytz.timezone(time_zone)
    localized_time = pytz.utc.localize(timeobj).astimezone(time_zone)

    return localized_time


def convert_to_datetime(timestamp_str):
    """
    Converts a string timestamp to a datetime object.
    
    Args:
        timestamp_str (str): The timestamp string in the format "%Y-%m-%d %H:%M:%S.%f %Z".
    
    Returns:
        datetime: The corresponding datetime object.
    """

    return datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f %Z")

current_time = "2023-01-25 18:13:22.47922 UTC"

current_time = convert_to_datetime(current_time)

global starting_time

csv_files = []

def last_hour(store_id,current_time,session,hours):

    """
    Retrieves activities within the last 'hours' for a specific store.
    
    Args:
        store_id (int): The store's ID.
        current_time (datetime): The current timestamp.
        session (Session): The database session.
        hours (int): Number of hours to consider.
    
    Returns:
        List[Activity]: List of activities within the last 'hours'.
    """

    activities = session.query(Activity).\
    filter(Activity.store_id == store_id).order_by(asc(Activity.timestamp_utc))

    activities_in_range = []
    global starting_time
    starting_time = current_time - timedelta(hours=hours)
    for activity in activities:
        if convert_to_datetime(activity.timestamp_utc) >= starting_time:
            if convert_to_datetime(activity.timestamp_utc) <= current_time:
                activities_in_range.append(activity)

    return activities_in_range

def find_business_hours(store_id,session):
    """
    gets business hours for each day of the week for a specific store.
    
    Args:
        store_id (int): The store's ID.
        session (Session): The database session.
    
    Returns:
        Business hours for each day of the week.
    """

    businessHours=session.query(BusinessHours).filter(BusinessHours.store_id == store_id)


    businessTime = {}
    for weekday in businessHours:

        if businessTime.get(weekday.day):
            businessTime[weekday.day].append({
                'start': weekday.start_time_local,
                'end': weekday.end_time_local,
            })
        else:
            businessTime[weekday.day]=[]
            businessTime[weekday.day].append({
                'start': weekday.start_time_local,
                'end': weekday.end_time_local,
            })
    if businessTime=={}:
        businessTime = {day: [{'start': '00:00:00', 'end': '23:59:59'}] for day in range(7)}
    return businessTime


def filter_polls(store_id , last_hour,businessTime,session):

    """
    Filters activities based on business hours and returns filtered data.
    
    Args:
        store_id (int): The store's ID.
        last_hour_activities (List[Activity]): List of activities within the last hour.
        business_hours (Dict[int, List[Dict[str, str]]]): Business hours for each day of the week.
        session (Session): The database session.
    
    Returns:
        Filtered activity data and total business hours time.
    """

    timeZones = session.query(Timezone).filter(Timezone.store_id == store_id)
    businessTime = find_business_hours(store_id,session)
    filtered_dates = []
    timings = []

    try:   
        if timeZones[0].timezone_str:
            timeZone = timeZones[0].timezone_str
    except:
        timeZone = "America/Chicago"

    for poll in last_hour:
        local_time = convert_to_local(convert_to_datetime(poll.timestamp_utc), timeZone)
        day_of_week = local_time.weekday()
        if businessTime.get(day_of_week):
            for slot in businessTime[day_of_week]:
                start_time = datetime.datetime.strptime(slot['start'], '%H:%M:%S').time()
                end_time = datetime.datetime.strptime(slot['end'], '%H:%M:%S').time()
                
                if start_time<= local_time.time() <=end_time:

                    if {"date":local_time.date(),"start_time":start_time, "end_time":end_time } not in timings:
                        timings.append({"date":local_time.date(),"start_time":start_time, "end_time":end_time })
                    # print(local_time," start_time " , start_time , "end_time ",end_time)
                    filtered_dates.append({"poll": poll.status ,"local_time" : local_time,"start_time":start_time, "end_time":end_time })  #"start_time":start_time, "end_time":end_time


    total_difference = timedelta()
    starting_time_local = convert_to_local(starting_time,timeZone).replace(tzinfo=None)

    if filtered_dates == []:
        return None,None

    if starting_time_local > datetime.datetime.combine(timings[0]['date'],timings[0]['start_time']):
        timings[0]['start_time']= starting_time_local.time()
    

    current_time_local = convert_to_local(current_time,timeZone).replace(tzinfo=None)

    if current_time_local < datetime.datetime.combine(timings[-1]['date'],timings[-1]['end_time']):
        timings[-1]["end_time"] = current_time_local.time()
    

    for entry in timings:
        start_time = datetime.datetime.combine(entry['date'], entry['start_time'])
        end_time = datetime.datetime.combine(entry['date'], entry['end_time'])
        time_difference = end_time - start_time
        total_difference += time_difference

    return filtered_dates,total_difference




def get_active_inactive(store_id, current_time,session,hours):
    """
    Calculates active and inactive hours within the last 'hours' for a specific store.
    
    Args:
        store_id (int): The store's ID.
        current_time (datetime): The current timestamp.
        session (Session): The database session.
        hours (int): Number of hours to consider.
    
    Returns:
        Active hours, inactive hours, and total business hours time.
    """

    data,total_difference = filter_polls(store_id,last_hour(store_id,current_time,session,hours),find_business_hours(store_id,session),session)

    print("total difference",total_difference) # total difference is the total business hours time
    if data==None:
        return 0,0,0
    active_hours = 0
    inactive_hours = 0

    """ 
    The active and inactive hours are calculated based on the differences between each entry
    if the difference between two entries is less than 1 hour 10 minutes, then based on the current entries status
    the value of the active and inactive hours are calculated. if the difference is greater than the threashold 
    the entries are discarded.
    """
    for i in range(len(data)):
        poll_status = data[i]['poll']
        local_time = data[i]['local_time']
        prev_time = data[i-1]['local_time']
        if poll_status == "active":

            if (abs(local_time - prev_time).total_seconds())<=4200:
                active_hours += abs((local_time - prev_time).total_seconds())
        else:
            if abs((local_time - prev_time).total_seconds())<=4200:
                inactive_hours += abs((local_time - prev_time).total_seconds())


    return active_hours, inactive_hours , total_difference.total_seconds()

def write_to_csv(folder_path, csv_id, data):
    csv_filename = os.path.join(folder_path, f"{csv_id}_data.csv")

    if not os.path.exists(csv_filename):
        with open(csv_filename, 'w', newline='') as file:
            writer = csv.writer(file)
            headers = ['uptime_last_hour', 'downtime_last_hour', 'uptime_last_day', 'downtime_last_day', 'uptime_last_week', 'downtime_last_week']
            writer.writerow(headers)

    with open(csv_filename, 'a', newline='') as file:
        writer = csv.writer(file)
        for row in data:
            writer.writerow(row)



def update_active_inactive(active,inactive,total_time,hour):
    """
    updates active and inactive hours based on remaining time and current active to inactive  ratio.
    the function get_active_inactive does not consider the business hours that haven't been recoreded by an entry
    so this function calculates the ratio of active and inactive hours,
    and calculates the active and inactive time periods based on the ratio calculated for the remaining time

    Args:
        active (float): Active hours.
        inactive (float): Inactive hours.
        total_time (float): Total time (business hours).
        hour (int): Number of hours being considered.
    
    Returns:
        Updated active and inactive hours.
    """
    combined_total = inactive+active
    remaining_time = total_time-combined_total
    if inactive>0 and active>0:
        combined_total = inactive+active
        active_ratio = active/combined_total
        remaining_active = active_ratio*remaining_time
        remaining_inactive = remaining_time-remaining_active

    elif inactive==0:
        remaining_active = remaining_time
        remaining_inactive = inactive
    
    else:
        remaining_active = active
        remaining_inactive = remaining_time

    total_active = active+remaining_active
    total_inactive = inactive+remaining_inactive
    
    if hour==1:
        return round(total_active/60,1),round(total_inactive/60,1)
    else:
        return  round(total_active/3600,1),round(total_inactive/3600,1)
    

def generate_report(session,current_time,csv_id,*hours):

    """
    Generates a report for the specified hours and stores the results in a CSV file.
    
    Args:
        session (Session): The database session.
        current_time (datetime): The current timestamp.
        csv_id (str): The ID for the CSV file.
        *hours (int): Variable number of hours to consider.
    
    Returns:
        str: The CSV ID.
    """
    
    unique_store_ids = (
        session.query(Activity.store_id)
        .distinct()
        .all()
    )
    
    store_ids = [store_id for (store_id,) in unique_store_ids]
    final_results=[]
    for store_id in store_ids[2000:2020]:
        results = [store_id]
        for hour in hours:
            active_in_hour,inactive_hour,hour_time = get_active_inactive(store_id, current_time,session,hour)
            updated_active, updated_inactive = update_active_inactive(active_in_hour,inactive_hour,hour_time,hour)

            results.extend([updated_active, updated_inactive])
        
        final_results.append(results)

    folder_path = "./results/"
    csv_files.append(csv_id)
    write_to_csv(folder_path, csv_id, final_results)
    
    return csv_id
        
