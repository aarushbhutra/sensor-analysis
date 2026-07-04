from copy import deepcopy


REQUIRED_ANOMALIES = {
    "temperature_high",
    "humidity_out_of_range",
    "soil_moisture_low",
    "co2_out_of_range",
    "light_out_of_range",
    "battery_low",
    "missing_data",
}


SCHEDULE = {
    0: "temperature_high",
    5: "humidity_out_of_range",
    11: "soil_moisture_low",
    17: "co2_out_of_range",
    23: "light_out_of_range",
    29: "battery_low",
    34: "missing_data",
}


def inject_anomaly(event: dict, event_index: int, thresholds: dict) -> dict:
    anomaly_type = SCHEDULE.get(event_index % 140)
    if anomaly_type is None:
        return event

    event = deepcopy(event)
    event["is_injected_anomaly"] = True
    event["anomaly_type"] = anomaly_type

    if anomaly_type == "temperature_high":
        event["temperature_c"] = thresholds["temperature_c"]["max"] + 4
        event["fan_status"] = "on"
    elif anomaly_type == "humidity_out_of_range":
        event["humidity_pct"] = max(0, thresholds["humidity_pct"]["min"] - 12)
    elif anomaly_type == "soil_moisture_low":
        event["soil_moisture_pct"] = max(0, thresholds["soil_moisture_pct"]["min"] - 15)
        event["irrigation_status"] = "on"
    elif anomaly_type == "co2_out_of_range":
        event["co2_ppm"] = thresholds["co2_ppm"]["max"] + 350
    elif anomaly_type == "light_out_of_range":
        event["light_lux"] = thresholds["light_lux"]["max_day"] + 5000
    elif anomaly_type == "battery_low":
        event["battery_level"] = max(0, thresholds["battery_level"]["min"] - 5)
    elif anomaly_type == "missing_data":
        event["location_status"] = "offline"

    return event
