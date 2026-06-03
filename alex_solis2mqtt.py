#!/usr/bin/python3

import argparse, time, traceback, os, minimalmodbus, yaml
import os
from datetime import datetime, timezone
from threading import Lock
from collections import deque

from pysolar.radiation import get_radiation_direct
from pysolar.solar import get_altitude
from pysolar.util import get_sunrise_sunset_transit

from config import Config
from inverter import Inverter
from mqtt import Mqtt
from mqtt_discovery import DiscoverMsgNumber, DiscoverMsgSensor, DiscoverMsgSwitch

CONFIG_FILE = "config.yaml"
SOLIS_MODBUS_CONFIG = "solis_modbus.yaml"
SUNSET_THRESHOLD = -10
VERSION = 2
"""
To do:
Split out specific write and read functions that
look up register addresses and funciton_codes, etc from the name of the item in solid_modbus.yaml
"""


class Solis2Mqtt:
    def __init__(self):
        self.cfg = Config("config.yaml")
        self.register_cfg = ...
        self.load_register_cfg()
        self.inverter = Inverter(self.cfg["device"], self.cfg["slave_address"])
        self.inverter_lock = Lock()
        self.inverter_offline = False
        self.mqtt = Mqtt(self.cfg["inverter"]["name"], self.cfg["mqtt"])
        self.past_values = {} # register_key:[oldest_value, newest_old_value, new_value]
        self.string_power = {

        } # string1:{last_power_amount:2,'last_power_time':time.time()}
        self.default_output_power = 10 # Do not update this value, its the default
        self.changed_output_power = 10 # when you receive a message to solisreaderwest/power_limitation/set, update this value
        self.changed_output_power_at = time.time()


    def load_register_cfg(self, register_data_file="solis_modbus.yaml") -> None:
        with open(register_data_file) as smfile:
            self.register_cfg = yaml.load(smfile, yaml.Loader)


    def update_string_power(self, string_name, string_voltage, string_current):
        now = time.time()
        last_time = self.string_power

    def generate_ha_discovery_topics(self) -> None:
        for entry in self.register_cfg:
            if entry["active"] and "homeassistant" in entry:
                topic = str(
                    f"homeassistant/{entry['homeassistant']['device']}"
                    + f"/{self.cfg['inverter']['name']}"
                    + f"/{entry['name']}/config"
                )
                if entry["homeassistant"]["device"] == "sensor":
                    self.mqtt.publish(
                        topic=topic,
                        payload=str(
                            DiscoverMsgSensor(
                                entry["description"],
                                entry["name"],
                                entry["unit"],
                                entry["homeassistant"].get("device_class",''),
                                entry["homeassistant"].get("state_class",''),
                                self.cfg["inverter"]["name"],
                                self.cfg["inverter"]["model"],
                                self.cfg["inverter"]["manufacturer"],
                                1,
                            )
                        ),
                        retain=True,
                    )
                elif entry["homeassistant"]["device"] == "number":
                    self.mqtt.publish(
                        topic=topic,
                        payload=str(
                            DiscoverMsgNumber(
                                entry["description"],
                                entry["name"],
                                entry["homeassistant"]["min"],
                                entry["homeassistant"]["max"],
                                entry["homeassistant"]["step"],
                                self.cfg["inverter"]["name"],
                                self.cfg["inverter"]["model"],
                                self.cfg["inverter"]["manufacturer"],
                                VERSION,
                            )
                        ),
                        retain=True,
                    )
                elif entry["homeassistant"]["device"] == "switch":
                    self.mqtt.publish(
                        topic=topic,
                        # f"homeassistant/switch/{self.cfg['inverter']['name']}"
                        # + f"/{entry['name']}/config",
                        payload=str(
                            DiscoverMsgSwitch(
                                entry["description"],
                                entry["name"],
                                entry["homeassistant"]["payload_on"],
                                entry["homeassistant"]["payload_off"],
                                self.cfg["inverter"]["name"],
                                self.cfg["inverter"]["model"],
                                self.cfg["inverter"]["manufacturer"],
                                VERSION,
                            )
                        ),
                        retain=True,
                    )

    def subscribe(self) -> None:
        for entry in self.register_cfg:
            if entry["active"] and "write_function_code" in entry["modbus"]:
                if not self.mqtt.on_message:
                    self.mqtt.on_message = self.on_mqtt_message
                topic = f"{self.cfg['inverter']['name']}/{entry['name']}/set"
                #print('subscription topic',topic)
                self.mqtt.persistent_subscribe(topic)

    def read_composed_date(self, register: int, functioncode: int) -> str:
        year = self.inverter.read_register(register[0], functioncode=functioncode)
        month = self.inverter.read_register(register[1], functioncode=functioncode)
        day = self.inverter.read_register(register[2], functioncode=functioncode)
        hour = self.inverter.read_register(register[3], functioncode=functioncode)
        minute = self.inverter.read_register(register[4], functioncode=functioncode)
        second = self.inverter.read_register(register[5], functioncode=functioncode)
        return f"20{year:02d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}"

    def update_power_limitation(self, new_limit):
        print('update_power_limitation',new_limit)
        try:
            new_limit = float(new_limit)
        except ValueError:
            print(f'{new_limit} is not floatable')
            return
        if new_limit < 0 or new_limit > 100:
            print(f'{new_limit} outside valid range')
        self.changed_output_power = new_limit
        self.changed_output_power_at = time.time()

    def actually_set_power_limitation(self):
        now = time.time()
        current_value = self.past_values.get('power_limitation', self.default_output_power)
        if now - self.changed_output_power_at > 10:
            write_value = self.default_output_power
        else:
            write_value = self.changed_output_power
        if int(write_value) != int(current_value):
            print(f'Setting output power to {write_value}')
            with self.inverter_lock:
                try:
                    self.inverter.write_register(
                        registeraddress=3051,
                        value=write_value,           
                        number_of_decimals=2,
                        functioncode=6,
                        signed=False
                    )   
                    time.sleep(1)
                except (minimalmodbus.NoResponseError, minimalmodbus.InvalidResponseError):
                    pass
                try:
                    value = self.inverter.read_register(
                        registeraddress=3051,  
                        number_of_decimals=2,
                        functioncode=3,
                        signed=False
                    )
                    print('value',value)
                    self.past_values['power_limitation'] = value
                except (minimalmodbus.NoResponseError, minimalmodbus.InvalidResponseError):
                    pass





    def on_mqtt_message(self, client, userdata, msg) -> None:
        if 'power_limitation/set' in msg.topic:
            self.update_power_limitation(msg.payload.decode('utf-8'))
            return
        for el in self.register_cfg:
            if el["name"] == msg.topic.split("/")[-2]:
                register_cfg = el["modbus"]
                break

        str_value = msg.payload.decode("utf-8")
        if "number_of_decimals" in register_cfg and register_cfg["number_of_decimals"] > 0:
            value = float(str_value)
        else:
            value = int(str_value)
        with self.inverter_lock:
            try:
                self.inverter.write_register(
                    register_cfg["register"],
                    value,
                    register_cfg["number_of_decimals"],
                    register_cfg["write_function_code"],
                    register_cfg["signed"],
                )
            except (minimalmodbus.NoResponseError, minimalmodbus.InvalidResponseError):
                pass

    def main(self):
        date = datetime.now(timezone.utc)
        solar_altitude = get_altitude(self.cfg["latitude"], self.cfg["longitude"], date)
        sunrise, sunset, sunhigh = get_sunrise_sunset_transit(
            latitude_deg=self.cfg["latitude"], longitude_deg=self.cfg["longitude"], when=date
        )
        solar_radiation_direct = get_radiation_direct(date, solar_altitude)


        self.generate_ha_discovery_topics()
        self.subscribe()

        for entry in self.register_cfg:
            if not entry["active"] or "function_code" not in entry["modbus"]:
                continue

            try:
                if entry["modbus"]["read_type"] == "register":
                    with self.inverter_lock:
                        value = self.inverter.read_register(
                            registeraddress=entry["modbus"]["register"],
                            number_of_decimals=entry["modbus"]["number_of_decimals"],
                            functioncode=entry["modbus"]["function_code"],
                            signed=entry["modbus"]["signed"],
                        )

                elif entry["modbus"]["read_type"] == "long":
                    with self.inverter_lock:
                        value = self.inverter.read_long(
                            registeraddress=entry["modbus"]["register"],
                            functioncode=entry["modbus"]["function_code"],
                            signed=entry["modbus"]["signed"],
                        )

                elif entry["modbus"]["read_type"] == "composed_datetime":
                    with self.inverter_lock:
                        value = self.read_composed_date(
                            register=entry["modbus"]["register"],
                            functioncode=entry["modbus"]["function_code"],
                        )
            
            # NoResponseError occurs if inverter is off,
            # InvalidResponseError might happen when inverter is starting up or
            # shutting down during a request
            except (minimalmodbus.NoResponseError, minimalmodbus.InvalidResponseError):

                # in case we didn't have a exception before
                self.inverter_offline = True

                if "homeassistant" in entry and entry["homeassistant"]["state_class"] == "measurement":
                    value = None
                else:
                    continue
            else:
                self.inverter_offline = False

            
            key = entry['name']
            print(key,value)
            if value is None:
                continue
            # Logic to not send 0s or Nones spuriously
            old_value = self.past_values.get(key)
            send_value = None
            if old_value is None:
                send_value = value
            elif old_value != value:
                send_value = value
            self.past_values[key] = value

            self.actually_set_power_limitation()
            
            if send_value is not None:
                self.mqtt.publish(f"{self.cfg['inverter']['name']}/{entry['name']}", send_value, retain=True)
            time.sleep(.5)

if __name__ == "__main__":

    try:
        s2m = Solis2Mqtt()
        while True:
             s2m.main()
            #time.sleep(5)
    except Exception:
        traceback.print_exc()
        exit(1)
