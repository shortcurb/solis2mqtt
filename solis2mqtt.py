import asyncio, json, minimalmodbus, datetime, yaml, random, time

from rolling_window import RollingWindow
from mqtt import Mqtt
from inverter import Inverter
from config import Config
from mqtt_discovery import DiscoverMsgNumber, DiscoverMsgSensor, DiscoverMsgSwitch

"""
Got it working while the inverter is on. Need to test edge cases like: MQTT disconnected, inverter disconnected, etc
"""

class ModbusManager:
    def __init__(self):
        self.class_name = self.__class__.__name__

        self.house_power_window = RollingWindow()
        self.east_inverter_window = RollingWindow()
        self.west_inverter_window = RollingWindow()

        self.cfg = Config("config.yaml")
        self.load_register_cfg()
        self.inverter_lock = asyncio.Lock()

        self.inverter = Inverter(self.cfg["device"], self.cfg["slave_address"])
        self.past_values = {} # register_key:[oldest_value, newest_old_value, new_value]
        self.inverter_online = False
        self.default_output_power = 8 # Do not update this value, its the default
        self.changed_output_power = 10 # when you receive a message to solisreaderwest/power_limitation/set, update this value
        self.changed_output_power_at = time.time()
        self.data_topic = 'sagehouse/electric/solar/{inverter_name}/{field_name}'
        self.inverter_name = self.cfg['inverter']['name']
        self.power_limitation_active = self.cfg['inverter']['power_limiting']


        self.topics = {
            f'sagehouse/electric/management/{self.inverter_name}/power_limitation/set':{'handler_function':self.handle_set_power,'retain':False}
        }

    async def generate_ha_discovery_topics(self) -> None:
        await asyncio.sleep(3)
        while True:
            for entry in self.register_cfg:
                if entry["active"] and "homeassistant" in entry:
                    topic = str(
                        f"homeassistant/{entry['homeassistant']['device']}"
                        + f"/{self.cfg['inverter']['name']}"
                        + f"/{entry['name']}/config"
                    )
                    if entry["homeassistant"]["device"] == "sensor":
                        await self.mqtt.publish_now(
                            topic=topic,
                            message=str(
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
                        await self.mqtt.publish_now(
                            topic=topic,
                            message=str(
                                DiscoverMsgNumber(
                                    entry["description"],
                                    entry["name"],
                                    entry["homeassistant"]["min"],
                                    entry["homeassistant"]["max"],
                                    entry["homeassistant"]["step"],
                                    self.cfg["inverter"]["name"],
                                    self.cfg["inverter"]["model"],
                                    self.cfg["inverter"]["manufacturer"],
                                    1,
                                )
                            ),
                            retain=True,
                        )
                    elif entry["homeassistant"]["device"] == "switch":
                        await self.mqtt.publish_now(
                            topic=topic,
                            # f"homeassistant/switch/{self.cfg['inverter']['name']}"
                            # + f"/{entry['name']}/config",
                            message=str(
                                DiscoverMsgSwitch(
                                    entry["description"],
                                    entry["name"],
                                    entry["homeassistant"]["payload_on"],
                                    entry["homeassistant"]["payload_off"],
                                    self.cfg["inverter"]["name"],
                                    self.cfg["inverter"]["model"],
                                    self.cfg["inverter"]["manufacturer"],
                                    1,
                                )
                            ),
                            retain=True,
                        )
            await asyncio.sleep(5)

    def read_composed_date(self, registeraddress: int, number_of_decimals: int, functioncode: int, signed: bool) -> str:
        year = self.inverter.read_register(registeraddress[0], functioncode=functioncode)
        month = self.inverter.read_register(registeraddress[1], functioncode=functioncode)
        day = self.inverter.read_register(registeraddress[2], functioncode=functioncode)
        hour = self.inverter.read_register(registeraddress[3], functioncode=functioncode)
        minute = self.inverter.read_register(registeraddress[4], functioncode=functioncode)
        second = self.inverter.read_register(registeraddress[5], functioncode=functioncode)
        return f"20{year:02d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}"

    async def _interact_modbus(self, method, reg_id, write_value, decimals, function_code):
        return_value = None
        async with self.inverter_lock:
            #print('reg_id', reg_id, 'method',method, 'write_value',write_value, 'decimals',decimals, 'function_code',function_code)
            try:
                if write_value is None:
                    args = {
                        'registeraddress':reg_id,
                        'number_of_decimals':decimals,
                        'functioncode':function_code,
                        'signed':False
                    }
                    if method == 'register':
                        return_value = self.inverter.read_register(**args)  
                    elif method == 'long':
                        del args['number_of_decimals']
                        return_value = self.inverter.read_long(**args) 
                    elif method == 'composed_datetime':
                        return_value = self.read_composed_date(**args) 
                    else:
                        print('unknown method')
                else:
                    self.inverter.write_register(
                        registeraddress=reg_id,
                        value=write_value,           
                        number_of_decimals=decimals,
                        functioncode=function_code,
                        signed=False
                    )     
                await self.set_inverter_online()
                #print('reg_id',reg_id,'return_value',return_value)
                return return_value
            except minimalmodbus.NoResponseError:
                print('no modbus response')
                await self.set_inverter_offline()
            except (minimalmodbus.InvalidResponseError):
                print('bad modbus response')
                await self.set_inverter_offline()
            return return_value

    async def read_register(self, method, reg_id, decimals, function_code):
        return_value = await self._interact_modbus(method, reg_id, None, decimals, function_code)
        return return_value

    async def write_register(self, reg_id, write_value, decimals, function_code):
        await self._interact_modbus(None, reg_id, write_value, decimals, function_code)

    async def simple_write(self, entry_name, write_value):
        entry = None
        for item in self.register_cfg:
            if item['name'] == entry_name:
                entry = item
                break
        if entry is not None:
            modbus_info = entry.get('modbus')
            #print(f'writing {write_value} to {entry_name} on register {modbus_info.get('register')}')
            await self.write_register(
                modbus_info.get('register'),
                write_value,
                modbus_info.get('number_of_decimals'),
                modbus_info.get('write_function_code')
            )
        else:
            print(f'unknown modbus entry {entry_name}')
            return False

    async def simple_read(self, entry_name):
        entry = None
        for item in self.register_cfg:
            if item['name'] == entry_name:
                entry = item
                break
        if entry is not None:
            modbus_info = entry.get('modbus')
            response = await self.read_register(
                modbus_info.get('read_type'),
                modbus_info.get('register'),
                modbus_info.get('number_of_decimals'),
                modbus_info.get('function_code')
            )
            return response
        else:
            print(f'unknown modbus entry {entry_name}')
            return False
        
    async def set_inverter_offline(self):
        if self.inverter_online:
            await self.mqtt.publish_now(self.data_topic.format(inverter_name = self.inverter_name, field_name = 'online'), False)
            #await self.mqtt.publish_now(f'{self.cfg['inverter']['name']}/online', False)
        self.inverter_online = False

    async def set_inverter_online(self):
        if not self.inverter_online:
            await self.mqtt.publish_now(self.data_topic.format(inverter_name = self.inverter_name, field_name = 'online'), True)
            #await self.mqtt.publish_now(f'{self.cfg['inverter']['name']}/online', True)
        self.inverter_online = True

    def load_register_cfg(self, register_data_file="solis_modbus.yaml") -> None:
        with open(register_data_file) as smfile:
            self.register_cfg = yaml.load(smfile, yaml.Loader)

    async def handle_set_power(self, topic, payload):
        if not self.power_limitation_active:
            return
        new_limit = payload
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
        print(topic, payload)

    async def power_limitation_worker(self):
        await asyncio.sleep(3)
        if not self.power_limitation_active:
            await self.simple_write('power_limitation', 100)
            return
        await self.simple_write('power_limitation',self.default_output_power)
        await asyncio.sleep(0.5)
        self.past_values['power_limitation'] = await self.simple_read('power_limitation')
        while True:
            now = time.time()
            current_value = self.past_values.get('power_limitation', None)
            if now - self.changed_output_power_at > 10:
                write_value = self.default_output_power
            else:
                write_value = self.changed_output_power
            if current_value is None:
                current_value = 0
            if int(write_value) != int(current_value):
                await self.simple_write('power_limitation',write_value)
                await asyncio.sleep(5) # sleep 5s for the write to complete and new values to be read
            await asyncio.sleep(0.1)

    async def dedupe_values(self, name, value):
        send_value = None
        if value is  None:
            return send_value
        # Logic to not send 0s or Nones spuriously
        old_value = self.past_values.get(name)
        send_value = None
        if old_value is None:
            send_value = value
        elif old_value != value:
            send_value = value
        self.past_values[name] = value
        return send_value

    async def polling_worker(self, entry):
        await asyncio.sleep(3)
        frequency = entry.get('polling_frequency',120)
        variation = entry.get('polling_variation',15)
        while True:
            send_value = None
            modbus_info = entry.get('modbus')
            response_value = await self.read_register(modbus_info.get('read_type'),modbus_info.get('register'), modbus_info.get('number_of_decimals', 1), modbus_info.get('function_code'))
            if entry.get('dedupe', True):
                send_value = await self.dedupe_values(entry.get('name'), response_value)
            else:
                send_value = response_value

            if send_value is not None:
                self.past_values[entry.get('name')] = send_value
                #await self.mqtt.publish_now(f"{self.cfg['inverter']['name']}/{entry['name']}", send_value, retain=False)
                await self.mqtt.publish_now(self.data_topic.format(inverter_name = self.inverter_name, field_name = entry['name']), send_value, retain = False)
            if variation:
                wait_time = frequency + random.randint(-1*int(variation/2), int(variation/2))
            else:
                wait_time = round(frequency + random.randint(-10, 10)/100,2)
            #if entry.get('name') == 'power_limitation':
                #print(entry.get('name'),wait_time, response_value)
            await asyncio.sleep(wait_time)

    async def create_reading_tasks(self):
        for entry in self.register_cfg:
            if entry.get("active",False):
                self.reading_tasks.append(self.polling_worker(entry))
        print(f'Created {len(self.reading_tasks)} reading tasks')

    async def run(self):
        self.mqtt = Mqtt(self.class_name, self.topics, max_worker_count = 10)
        self.reading_tasks = [
            self.mqtt.listener(),
            self.power_limitation_worker(),
            self.generate_ha_discovery_topics()
        ]
        await self.create_reading_tasks()
        await asyncio.gather(*self.reading_tasks),




mm = ModbusManager()
asyncio.run(mm.run())