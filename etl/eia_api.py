import pandas as pd
import datetime
import requests



def day_offset(start, end, offset):
    """
    Generate a list of dates with a specified day offset between a start and end date.

    Parameters:
    start (datetime.date): The start date.
    end (datetime.date): The end date.
    offset (int): The number of days to offset.

    Returns:
    list: A list of dates from start to end with the specified offset.
    """
    current = [start]
    while max(current) < end:
        if(max(current) + datetime.timedelta(days= offset) < end):
            current.append(max(current) + datetime.timedelta(days= offset))
        else:
            current.append(end) 
    return current

def hour_offset(start, end, offset):
    """
    Generate a list of datetimes with a specified hour offset between a start and end datetime.

    Parameters:
    start (datetime.datetime): The start datetime.
    end (datetime.datetime): The end datetime.
    offset (int): The number of hours to offset.

    Returns:
    list: A list of datetimes from start to end with the specified offset.
    """
    current = [start]
    while max(current) < end:
        if(max(current) + datetime.timedelta(hours = offset) < end):
            t = max(current) + datetime.timedelta(hours = offset)
            if t.hour == 0:
                t = t + datetime.timedelta(hours = 1)
            current.append(t)
        else:
            current.append(end) 
    return current




class Metadata:
    def __init__(self, meta, url, parameters):
        self.meta = meta
        self.url = url
        self.parameters = parameters

def eia_metadata(api_key, api_path=None):
    """
    Fetch metadata from the EIA API.

    Parameters:
    api_key (str): The API key for accessing the EIA API. Must be a 40-character string.
    api_path (str, optional): The specific API path to access. Defaults to None.

    Returns:
    Metadata: An object containing the metadata, URL, and parameters used for the request.

    Raises:
    ValueError: If the api_key is not a valid string or if its length is not 40 characters.
    """
    if not isinstance(api_key, str):
        raise ValueError("The api_key argument must be a valid string")
    if len(api_key) != 40:
        raise ValueError("The length of the api_key must be 40 characters")

    if api_path is None:
        url = "https://api.eia.gov/v2/?api_key="
    else:
        if not api_path.endswith("/"):
            api_path += "/"
        url = f"https://api.eia.gov/v2/{api_path}&api_key="

    response = requests.get(url + api_key)
    response.raise_for_status()  # Raise an error for bad status codes
    data = response.json()

    parameters = {
        "api_path": api_path
    }

    return Metadata(meta=data["response"], url=url, parameters=parameters)


class Response:
    def __init__(self, data, url, parameters):
        self.data = data
        self.url = url
        self.parameters = parameters



def eia_get(api_key, 
            api_path, 
            data="value", 
            facets=None, 
            start=None, 
            end=None, 
            length=None, 
            offset=None, 
            frequency=None):
    """
    Fetch data from the EIA API.

    Parameters:
    api_key (str): The API key for accessing the EIA API. Must be a 40-character string.
    api_path (str): The specific API path to access.
    data (str, optional): The data field to retrieve. Defaults to "value".
    facets (dict, optional): The facets to filter the data. Defaults to None.
    start (datetime.date or datetime.datetime, optional): The start date or datetime. Defaults to None.
    end (datetime.date or datetime.datetime, optional): The end date or datetime. Defaults to None.
    length (int, optional): The length of the data to retrieve. Defaults to None.
    offset (int, optional): The offset for the data retrieval. Defaults to None.
    frequency (str, optional): The frequency of the data. Defaults to None.

    Returns:
    Response: An object containing the data, URL, and parameters used for the request.

    Raises:
    ValueError: If the api_key is not a valid string or if its length is not 40 characters.
    """
    if not isinstance(api_key, str):
        raise ValueError("The api_key argument must be a valid string")
    if len(api_key) != 40:
        raise ValueError("The length of the api_key must be 40 characters")

    if not api_path.endswith("/"):
        api_path += "/"

    fc = ""
    if facets:
        for key, value in facets.items():
            if isinstance(value, list):
                for item in value:
                    fc += f"&facets[{key}][]={item}"
            elif isinstance(value, str):
                fc += f"&facets[{key}][]={value}"

    start_temp = start - datetime.timedelta(days = 1) 
    end_temp = end + datetime.timedelta(days = 1)

    s = f"&start={start_temp.strftime('%Y-%m-%d')}" if isinstance(start_temp, datetime.date) else f"&start={start_temp.strftime('%Y-%m-%dT%H')}" if isinstance(start_temp, datetime.datetime) else ""
    e = f"&end={end_temp.strftime('%Y-%m-%d')}" if isinstance(end_temp, datetime.date) else f"&end={end_temp.strftime('%Y-%m-%dT%H')}" if isinstance(end_temp, datetime.datetime) else ""
    l = f"&length={length}" if length else ""
    o = f"&offset={offset}" if offset else ""
    fr = f"&frequency={frequency}" if frequency else ""

    url = f"https://api.eia.gov/v2/{api_path}?data[]={data}{fc}{s}{e}{l}{o}{fr}"

    response = requests.get(url + "&api_key=" + api_key)
    response.raise_for_status()  # Raise an error for bad status codes
    data = response.json()

    df = pd.DataFrame(data['response']['data'])
    if not df.empty:
        df = df.rename(columns={"period": "index"})
        df["index"] = pd.to_datetime(df["index"])
        df["value"] = pd.to_numeric(df["value"])
        df = df.sort_values(by=["index"])
        if start and isinstance(start, (datetime.date, datetime.datetime)):
            df = df[df["index"] >= start]
        if end and isinstance(end, (datetime.date, datetime.datetime)):
            df = df[df["index"] <= end]
        status = True
    else:
        status = False
        print("The return object does not contain observations, check the end parameter settings")

    parameters = {
        "api_path": api_path,
        "facets": facets,
        "start": start,
        "end": end,
        "length": length,
        "offset": offset,
        "frequency": frequency,
        "status": status
    }

    return Response(data=df, url=url + "&api_key=", parameters=parameters)