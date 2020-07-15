#CLUSTERED
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

clustered_list1a = gen(1300*2, 5, 18, 4, 22)
clustered_list2a = gen(1800*2, 17, 30, 37, 58 )
clustered_list3a = gen(2700*2, 35, 59, 10, 30)
clustered_list4a = gen(2200*2, 19, 55, 66, 78)
clustered_list5a = gen(2000*2, 57, 68, 58, 80)

points = clustered_list1a+clustered_list2a+clustered_list3a+clustered_list4a+clustered_list5a

clustered_list1b = gen(1300*2, 5, 18, 4, 22)
clustered_list2b = gen(1800*2, 17, 30, 37, 58 )
clustered_list3b = gen(2700*2, 35, 59, 10, 30)
clustered_list4b = gen(2200*2, 19, 55, 66, 78)
clustered_list5b = gen(2000*2, 57, 68, 58, 80)

points2 = clustered_list1b+clustered_list2b+clustered_list3b+clustered_list4b+clustered_list5b