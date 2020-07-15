#SKEWED
import random
import string


def gen(quantity, minlat, maxlat, minlon, maxlon):
    points = []
    i = 0
    while i<quantity:
        lat = round(random.uniform(minlat,maxlat), 6)
        lon = round(random.uniform(minlon,maxlon), 6)
        nam = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))

        points.append((nam,lat,lon))
        i += 1
    return points

skewed_list_1a = gen(7000*2, 0, 20, 0 , 80)
skewed_list_1b = gen(3000*2, 20, 80, 0, 80)
points = skewed_list_1a + skewed_list_1b

skewed_list_2a = gen(7000*2, 0, 20, 0 , 80)
skewed_list_2b = gen(3000*2, 20, 80, 0, 80)
points2 = skewed_list_2a + skewed_list_2b