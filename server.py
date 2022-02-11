import time
import math

brokerIP="127.0.0.1"

ja tässä on vielä tuo server.property


def factor(number):
    result = []
    for i in range(1,math.ceil(number/2)+1):
        if number/i%1==0:
            result.append(i)
    result.append(number)
    return result

if __name__=="__main__":
    print(factor(10**7+3))
