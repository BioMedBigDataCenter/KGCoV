continents = ['Eurasia',
 'Europe',
 'North-America',
 'Afirica',
 'South America',
 'North America',
 'Asian',
 'Central America',
 'Asia',
 'North america',
 'Europa',
 'Oceania',
 'America',
 'Africa',
 'Noth America',
 'South American',
 '']

def get_genome_location(location):
    try:
        location_split = location.split('/')[::-1]
        location_drop = [loc.strip() for loc in location_split if loc.strip() not in continents]
        if len(location_drop) != 1:
            location_qc = ','.join(location_drop)
        elif len(location_drop) == 1:
            location_qc = None
        return location_qc
    except AttributeError:
        location_qc = None
        return location_qc
    

# how to drop genome continent in location
# 