import googlemaps
import math

class Google_Maps:

    def __init__(self, API_key):
        self.gmaps = googlemaps.Client(key=API_key)


    def google_maps_request_directions(self, Coordinate_1, Coordinate_2):
        Lat_1, Long_1 = Coordinate_1
        Lat_2, Long_2 = Coordinate_2
        try:
            directions_result = self.gmaps.directions(f'{Lat_1},{Long_1}',
                                                 f'{Lat_2},{Long_2}',
                                                  mode='driving',
                                                  language = 'en',
                                                  units = 'metric',
                                                  avoid='ferries')
            distance = directions_result[0]['legs'][0]['distance']['value']/1000
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
