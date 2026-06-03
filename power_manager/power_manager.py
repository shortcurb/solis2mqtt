import json, random, time, asyncio
from typing import Callable, Any
from collections import deque
from rolling_window import RollingWindow
from mqtt import Mqtt


class PowerManager:
    """
    Callback signature:
        callback(topic: str, payload: Any, raw_message: mqtt.MQTTMessage)
    """

    def __init__(self):
        self.class_name = self.__class__.__name__
        self.house_power_window = RollingWindow()
        self.east_inverter_window = RollingWindow()
        self.west_inverter_window = RollingWindow()

        self.export_max = 7600

        self.topics = {
            'shellypro3em-6825ddd2b968/power/total_power':{'handler_function':self.manage_house_power},
            'solisreaderwest/active_power':{'handler_function':self.manage_west_inverter},
            'solis2mqtt/active_power':{'handler_function':self.manage_east_inverter}
        }


    async def manage_house_power(self, topic, payload):
        payload = json.loads(payload)
        value = float(payload.get('total_power', None))
        self.house_power_window.add(value)

    async def manage_west_inverter(self, topic, payload):
        value = float(payload)
        self.west_inverter_window.add(value)

    async def manage_east_inverter(self, topic, payload):
        value = float(payload)
        self.east_inverter_window.add(value)

    async def make_recommendations(self):
        while True:
            change_production = None
            change_consuption = None
            await asyncio.sleep(0.5)
        """
        chat was right, you need to define boundaries for acceptable outputs so you avoid harmonic resonances between values
        """


    """
    What is the goal of this analysis?
    Control both max solar production and max consumption
    Is it better to do some kind of ratcheting up?
    Like this thing loops through and only recommends "increase production" and / or "increase consumption", and / or "emergency max default"?
    What are my limitations?
    First: the equation
    meter flow = house production - house consumption
    1. Meter flow must not be less than -7.6kW (remember "less than" means further left of 0, and we defined negative meter flow as produciton > consumption)
    2. Maximize house production

    Failing 1 means an immediate game failure, so it must be considered at all times
    There's no real "failing" or "winning" 2, you just have to get asymptotically close to it

    Variables I can control:
    West inverter production
    East inverter production
    Tesla charging (future)
    Crypto mining (future)

    Variables I don't control:
    Clouds
    The sun

    First off: rewrite the solis2mqtt to be less dumb. Don't loop through the whole solis_modbus.yaml config
    Create some kind of queue for when to query items based on their relevance. I care a lot about active_power, I care very little about "power last year"
    Damn dude chatGPT just gave me a script that makes the Shelly device fire active_power data every half-second, that's huge
    Chat also thinks the inverter export limit ramping can take a long time
    So really this controller should be set up to control consumption (cryptominer) loads within 1-2 second window
    Reducing production can happen within 1-2 seconds, easy
    But the MPPT algorithm takes 2-10 seconds to ramp back up. 
    Either way, well within my time windows

    """





    async def run(self):
        self.mqtt = Mqtt(self.class_name, self.topics, max_worker_count = 10)

        tasks = [
            self.mqtt.listener(),
            self.make_recommendations()
        ]
        await asyncio.gather(*tasks)

pm = PowerManager()
asyncio.run(pm.run())
