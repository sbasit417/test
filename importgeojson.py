#importing geojson to database table
import json
import psycopg2


with open('geojson.json') as file:
    json_data = json.load(file)

conn = psycopg2.connect(dbname="postgres", user = "postgres", password = "password", host = "127.0.0.1", port = "5432")	
cur = conn.cursor()


for data in json_data['features']:

    
    name = data['properties']['name']
    parent=data['properties']['parent']
    loc_type = data['properties']['type']

    for coordinate in data['geometry']['coordinates'][0]:    
        lng = coordinate[0]
        lat = coordinate[1]
       
        cur.execute("INSERT INTO boundary (name,type,parent,latitude,longitutde) VALUES ('"+name+"','"+loc_type+"','"+parent+"',"+str(lat)+","+str(lng)+");")
	
    print('Data Added successfully')
           			
conn.commit()
