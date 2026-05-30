import random, time

previous_number = []

while True:
    number = random.randint(-1,1)
    print(previous_number, number)
    previous_number.append(number)
    if previous_number[2] == 0 and previous_number[1] == 0 and previous_number[0] == 0 and number == 0:
        continue

    if len(previous_number) > 3:
        previous_number.pop(0)

    if len(previous_number) == 3:
        if previous_number[2] == 0 and previous_number[1] == 0 and previous_number[0] == 0:
            print('all zeroes')
        elif previous_number[2] != previous_number[1]:
            print('new number is different')
    time.sleep(.25)
        