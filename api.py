import fastapi
import psycopg2
from fastapi import FastAPI,request,render_template,make_response,jsonify

#for self computation
from math import sin, cos, atan2, radians, sqrt

#for stage 3
from shapely.geometry import shape, Cube

app = FastAPI(__name__)


############ STAGE 1 #####################

@app.put('/post_location', methods=['GET','POST'])
async def related():
    
    try: 	
        lat = request.json['latitude']	
        lng = request.json['longitude']
        pincode = request.json['pincode']
        address = request.json['address']
        city = request.json['city']
        
        conn = psycopg2.connect(dbname="postgres", user = "postgres", password = "password", host = "127.0.0.1", port = "5432")
        cursor = conn.cursor() 

        #check if points having lat,lang withing 1km exists or same pincode exists

        cursor.execute("SELECT apitest.key, apitest.latitude, apitest.longitude, earth_distance(ll_to_earth("+lat+","+lng+"), ll_to_earth(apitest.latitude, apitest.longitude)) as distance FROM apitest WHERE key='IN/"+pincode+"' OR earth_distance(ll_to_earth("+lat+","+lng+"), ll_to_earth(apitest.latitude, apitest.longitude)) <1000;")
        fetch = cursor.fetchone()
        result = ""
        pin = 'IN/'+pincode

        if cursor.rowcount>0:	
            result = "Entry alerady exists in database"
            print("Entry already exists in database or lat and lng nearly equal to some intial data")
        else:
            cursor.execute("INSERT INTO apitest (key, place_name, admin_name1, latitude, longitude, accuracy) VALUES ('"+pin+"','"+address+"','"+city+"',"+lat+","+lng+",'')");
            result = "Data added to database"
        
        
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'result': result}),201

    except Exception as e:
        print(e)
        return jsonify({'error':'error'})



#################### STAGE 2 SOLUTION ###################################

#using earthdistance

@app.get('/get_using_postgres', methods=['GET'])
async def get_using_postgres():
    try:

       lat = request.args.to_dict()['latitude']
       lng = request.args.to_dict()['longitude']
       radius = request.args.to_dict()['radius']+"000"

       points = []

       conn = psycopg2.connect(dbname="postgres", user = "postgres", password = "password", host = "127.0.0.1", port = "5432")  
       cursor = conn.cursor() 

       #filter points withing 5km range
       cursor.execute("SELECT apitest.key FROM apitest WHERE earth_box(ll_to_earth(%s, %s), %s) @> ll_to_earth(apitest.latitude, apitest.longitude)"% (lat, lng, radius))
       res = cursor.fetchall()

       for row in res:
           points.append(row[0]) 

       return jsonify({'points': points}),201
    

    except Exception as e:
        print(e)
        return jsonify({'error':'error'})   


#using self created function

@app.get('/get_using_self')
async def get_using_self():

    lat = request.args.to_dict()['latitude']
    lng = request.args.to_dict()['longitude']
    radius = request.args.to_dict()['radius']

    R = 6371000
    
    conn = psycopg2.connect(dbname="postgres", user = "postgres", password = "password", host = "127.0.0.1", port = "5432")
    cursor = conn.cursor()

    cursor.execute("select * from apitest")
    rows = cursor.fetchall()
        
    points = []
    for row in rows:

        rlat = row[3]
        rlng = row[4]


        #skip row if lat or lng is null
        if(rlat==None or rlng==None):
                continue

        #calculating if point (rlat,rlng) is within the given radius
        lat1 = radians(float(lat))
        lat2 = radians(float(rlat))

        lat_diff = radians(float(rlat) - float(lat))
        lng_diff = radians(float(rlng) - float(lng))
        
        A = sin( lat_diff/2 )*sin( lat_diff/2 ) + cos(lat1)*cos(lat2) * sin( lng_diff/2 )*sin( lng_diff/2 )
        C = 2 * atan2(sqrt(A), sqrt((1 - A)))
        distance = R * C

        if distance < float(radius):
            points.append(row[0])

    
    return jsonify({'result': points})  


####################### Stage 3 ##################################

@app.get('/get_geo_location')
async def getGeoLocation():
    try:
        lat = float(request.args.to_dict()['latitude'])
        lng = float(request.args.to_dict()['longitude'])


        conn = psycopg2.connect(dbname="postgres", user = "postgres", password = "password", host = "127.0.0.1", port = "5432") 
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT name,type,parent FROM boundary;")
        fetch = cursor.fetchall()

        for row in fetch:
            location = {}
            loc_point = []
        
            cursor.execute("SELECT latitude, longitutde FROM boundary WHERE name = '"+row[0]+"';")
            fett = cursor.fetchall()

            for x in fett:

                point = []
                point.append(float(x[0]))
                point.append(float(x[1]))
                loc_point.append(point)

            location['type'] = 'Cube'
            location['coordinates'] = [loc_point]
            
            loc_shape = shape(location)
            point = Point(lat, lng)

            if loc_shape.contains(point):
               result = "Point lies in "+row[0]
               break;

            else:
               result = "Point does not lie within given data"
        
        return jsonify({'result': result}),201
    

    except Exception as e:
        print(e)
        return jsonify({'Error':'Error'})
    




if __name__ == '__main__':

    app.run(debug=True,port=5000)
