import requests
import math

class OSRM_API:

    def __init__(self):
        self.url = 'http://router.project-osrm.org'


    def request_directions(self, Lat_1, Long_1, Lat_2, Long_2):
        coordinate_1 = f'{Long_1},{Lat_1}'
        coordinate_2 = f'{Long_2},{Lat_2}'
        service = 'route'
        version = 'v1'
        profile = 'driving'
        try:
            query = self.url + f'/{service}/{version}/{profile}/{coordinate_1};{coordinate_2}?overview=false'
            result = requests.get(query)
            if result.status_code == 200:
                distance = result.json()['routes'][0]['legs'][0]['distance']/1000
            else:
                distance = self.harvesine_formula(Lat_1, Long_1, Lat_2, Long_2)
        except IndexError:
            distance = self.harvesine_formula(Lat_1, Long_1, Lat_2, Long_2)
        return distance


    def harvesine_formula(self, Lat_1, Long_1, Lat_2, Long_2):
        Average_earth_radius = 6371
        phi1, phi2 = math.radians(Lat_1), math.radians(Lat_2)
        dphi = math.radians(Lat_2 - Lat_1)
        dlambda= math.radians(Long_2 - Long_1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        return 2*Average_earth_radius*math.atan2(math.sqrt(a), math.sqrt(1 - a))
