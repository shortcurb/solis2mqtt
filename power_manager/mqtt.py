import asyncio, fnmatch, json, traceback, time, random, os
from gmqtt import Client as MQTTClient, Message
from dotenv import load_dotenv

class Mqtt:
    def __init__(self, mqtt_user='default', topics={}, max_worker_count:int = 10):
        load_dotenv()
        self.client_name = f"{mqtt_user}-{random.randint(1000,9999)}"
        self.client = MQTTClient(self.client_name)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.set_config({'reconnect_retries': float('inf')})
        self.handlers = {}
        self.workers = []
        self.workers_lock = asyncio.Lock()
        self.max_worker_count = max_worker_count
        self.message_queue = asyncio.Queue(maxsize=10000)

        self.publish_queue = asyncio.Queue()

        self.function_name = self.__class__.__name__

        for topic, callback_info in topics.items():
            if callback_info.get('handler_function','printer') == 'printer':
                self.handlers[topic] = {'handler_function':self.printer,'retain':callback_info.get('retain',False)}
            else:
                self.handlers[topic] = callback_info

        self.is_connected = None

    async def printer(self, topic, message):
        print(topic, json.dumps(json.loads(message), indent=2, default=str))

    def on_connect(self, client, flags, rc, properties):
        asyncio.create_task(self.add_worker())
        asyncio.create_task(self.subscribe_to_handlers())

    async def connect(self):
        if not self.is_connected:
            self.client.clean_session = True
            self.client.set_auth_credentials(os.getenv('LOCAL_MQTT_USERNAME'), os.getenv('LOCAL_MQTT_PASSWORD'))
            await self.client.connect(os.getenv('LOCAL_MQTT_HOST'), int(os.getenv('LOCAL_MQTT_PORT')))
            print(f"Successfully connected to MQTT with user {self.client_name}")
            self.is_connected = True

    def on_disconnect(self, client, packet, exc=None):
        traceback.print_exc()
        print(f"[MQTT] Disconnected from broker. Exception: {exc}")
        self.is_connected = False
        # gmqtt will auto-reconnect unless you call disconnect()

    async def disconnect(self):
        await self.client.disconnect()
        self.is_connected = False

    async def subscribe(self, topic: str, callback):
        self.handlers[topic]['handler_function'] = callback
        self.client.subscribe(topic)

    async def publish_now(self, topic: str, message: str, response_topic: bool = False, qos: int = 1, retain: bool = True):
        msg = Message(topic, message, qos=qos, retain = retain)
        if response_topic:
            msg.response_topic = response_topic  # must be a topic string (MQTT v5)
        self.client.publish(msg,retain = retain)

    async def publish_queued(self, topic: str, message: str, response_topic: bool = False, qos: int = 1, retain: bool = True):
        msg = Message(topic, message, qos=qos, retain = retain)
        msg.retain = retain
        if response_topic:
            msg.response_topic = response_topic  # must be a topic string (MQTT v5)
        # Puts the messsage to be sent in an asyncio queue. Make sure mqtt.publisher_worker is one of your async.gather tasks!
        await self.publish_queue.put(msg)

    async def publisher_worker(
        self,
        stop_after_queue_empty: bool = False,
        timeout: float = 0.2,
        max_in_flight: int = 1000,          # tune: 50–500 depending on broker/latency
        publish_ack_timeout: float = 10.0  # safety: don't hang forever
    ):
        sem = asyncio.Semaphore(max_in_flight)
        pending: set[asyncio.Task] = set()

        async def _one(msg):
            async with sem:
                # IMPORTANT: call publish with topic/payload, not Message object
                fut = self.client.publish(
                    msg.topic,
                    msg.payload if hasattr(msg, "payload") else msg.message,
                    qos=getattr(msg, "qos", 1),
                    retain=getattr(msg, "retain", False),
                )

                # gmqtt typically returns a Future for QoS 1/2. Await it with a timeout.
                if fut is not None:
                    await asyncio.wait_for(fut, timeout=publish_ack_timeout)
                else:
                    # QoS 0 or non-future path: still yield to let the writer flush
                    await asyncio.sleep(0)

        while True:
            try:
                msg = await asyncio.wait_for(
                    self.publish_queue.get(),
                    timeout=timeout if stop_after_queue_empty else None,
                )
            except asyncio.TimeoutError:
                break

            t = asyncio.create_task(_one(msg))
            pending.add(t)
            t.add_done_callback(pending.discard)

        if pending:
            await asyncio.gather(*pending)

    async def subscribe_to_handlers(self):
        for topic, callback_info in self.handlers.items():
            await self.subscribe(topic, callback_info['handler_function'])

    async def add_worker(self):
        async with self.workers_lock:
            if len(self.workers) < self.max_worker_count:
                worker_id = random.randint(100000,999999)
                asyncio.create_task(self.message_worker(worker_id))
                self.workers.append(worker_id)
            else:
                print(f"Reached worker limit of {self.max_worker_count}")

        return

    async def message_worker(self, worker_id):
        #print(f"[MQTT] Worker {worker_id} starting")
        while True:
            try:
                topic,payload,callback = await asyncio.wait_for(self.message_queue.get(), timeout=5)
                try:
                    await callback(topic, payload)
                except Exception as e:
                    print(f"[MQTT] Error processing message for topic {topic}: {e}")
                finally:
                    self.message_queue.task_done()
            except asyncio.TimeoutError:
                async with self.workers_lock:
                    if len(self.workers) > 1:
                        #print(f"[Worker {worker_id}] Idle timeout, shutting down")
                        self.workers.remove(worker_id)
                        break

            await asyncio.sleep(0.01)

    async def on_message(self, client, topic, payload, qos, properties):
        payload = payload.decode()
        matched = False
        for pattern, callback_info in self.handlers.items():
            handler_retain = callback_info.get('retain',False)
            callback = callback_info['handler_function']
            if self._match_topic(pattern, topic):
                message_retain = properties['retain']
                if not handler_retain and message_retain:
                    return
                try:
                    self.message_queue.put_nowait((topic, payload, callback))
                except asyncio.QueueFull:
                    if len(self.workers) < self.max_worker_count:
                        print('queue full, adding worker')
                        await self.add_worker()
                    else:
                        print(f"[MQTT] Queue full and max worker limit reached")
                matched = True
                break

        if not matched:
            print(f"[MQTT] No handler found for {topic}")

    def _match_topic(self, pattern, topic):
        pattern = pattern.replace("+", "*").replace("#", "**")
        return fnmatch.fnmatch(topic, pattern)

    async def listener(self):
        # Make sure to wait .5 second before publishing if running this as a create_task
        await self.connect()
        await asyncio.sleep(0.5)
        asyncio.create_task(self.worker_manager())
        await asyncio.Event().wait()

    async def worker_manager(self):
        while True:
            queue_size = self.message_queue.qsize()
            worker_pool_qty = len(self.workers)
            if queue_size > 10 and worker_pool_qty < self.max_worker_count:
                like_to_create = queue_size - worker_pool_qty
                for _ in range(like_to_create):
                    await self.add_worker()
            await asyncio.sleep(.1)


async def run():
    topics = {
        'flespi/log/gw/devices/+/commands-queue/setting.data_services.get/processed':{'handler_function':'printer'},
    }
    mqtt = Mqtt('util_examine_mqtt',topics, max_worker_count = 5000)
    time.sleep(1)
    await mqtt.listener()


if __name__ == '__main__':
    asyncio.run(run())

