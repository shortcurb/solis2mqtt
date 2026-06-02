import minimalmodbus, time
import serial

PORT = "/dev/ttyUSB0"
SLAVE_ID = 1

instrument = minimalmodbus.Instrument(PORT, SLAVE_ID)

instrument.serial.baudrate = 9600
instrument.serial.bytesize = 8
instrument.serial.parity = serial.PARITY_NONE
instrument.serial.stopbits = 1
instrument.serial.timeout = 2

instrument.mode = minimalmodbus.MODE_RTU
instrument.clear_buffers_before_each_transaction = True
instrument.close_port_after_each_call = True

# Read holding register

def read_register(register_id):
    value = instrument.read_register(
        registeraddress=register_id,   # example only
        number_of_decimals=2,
        functioncode=4,
        signed=False
    )
    print(register_id,'=',value)
    time.sleep(0.5)

    return value

def write_register(register_id, value):
    print('writing',value,'to',register_id)
    instrument.write_register(
        registeraddress=register_id,
        value=value,           
        number_of_decimals=2,
        functioncode=6,
        signed=False
    )   
    time.sleep(0.5)
# read and write to 3049 in normal percents (10 = 10%)! confirmed works!

for i in [3049]:
    value = 10
    read_register(i)
    #write_register(i, value)
    #read_register(i)
    print('\n')
