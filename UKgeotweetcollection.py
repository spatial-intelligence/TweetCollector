

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time
import random
import gc
import psycopg2
import sys
import datetime
from datetime import datetime, timedelta

#reload(sys)  
#sys.setdefaultencoding('utf8')


consumer_key=[]
consumer_secret=[]
access_token=[]
access_token_secret=[]


#V1
consumer_key.append("put details here")
consumer_secret.append("put details here")
access_token.append("put details here")
access_token_secret.append("put details here")




Coords = dict()
Place = dict()
PlaceCoords = dict()
XY = []
global_counter = 0



class StdOutListener(StreamListener):
                """ A listener handles tweets that are the received from the stream. 
                This is a basic listener that writes tweets to disk or DB
                """

                def on_status(self, status):
                                
                                geosource='n/a'
                                bbox='gnss'

                                ##check for long tweet msgs with 'full text'
                                try:
                                    try:
                                        text = status.extended_tweet["full_text"]
                                    except AttributeError:
                                        text = status.text
                                except:
                                    text='[blank msg]'

        
                                try:
                                    Coords.update(status.coordinates)
                                    XY = (Coords.get('coordinates'))  #Place the coordinates values into a list 'XY'
                                    geosource='coords'
                                    #print "X: ", XY[0]
                                    #print "Y: ", XY[1]
                                except:
                                    #Often times users opt into 'place' which is neighborhood size polygon
                                    #Calculate simple centroid of bounding box
                                    try:
                                        
                                        Box = status.place.bounding_box.coordinates[0]
                                        bbox=Box

                                        XY = [(Box[0][0] + Box[2][0])/2, (Box[0][1] + Box[2][1])/2]
                                        geosource='place'
                                    except:
                                        XY=[-999.99,-999.99]



                                try:
                                    text=text.replace('\n','^').replace('\r','^').replace(',','|').replace('"','^')

                                    cleanmsg=text  #.encode('utf-8')
                                    cleanscreenname=status.user.screen_name.replace(',','-').replace('"','^')#.encode('utf-8')
                                    
                                    replytoid= 0

                                    try:
                                        if (status.in_reply_to_status_id ):
                                            replytoid=int(status.in_reply_to_status_id)
                                            
                                        lang=status.user.lang#.encode('utf-8')                                                  
                                        data = (status.created_at,lang, cleanmsg,bbox,str(XY[1]),str(XY[0]),str(status.created_at),status.id_str,status.user.name.replace(',','-').replace('"','^'),cleanscreenname, status.source,replytoid)
                                        print (data)
                                             

                                        
                                    except Exception as e:
                                        print (e)
                                        print ("data collection error")
                                        


                                    XY = None
                                    text= None
                                    rec= None 

                                    if (global_counter%30000)==0:
                                        print (datetime.now())
                                        gc.collect()
                                        
                                except Exception as e:
                                        print ('err>',datetime.now())
                                        print (e)
                      


def main():
    print ('--- started collector ---')
    collect()

    

    
def collect():
    ta=0
   
    try:
        print ('starting up...' + str(datetime.now()))
    
        l = StdOutListener()    
        auth = OAuthHandler(consumer_key[ta], consumer_secret[ta])
        auth.set_access_token(access_token[ta], access_token_secret[ta])
        stream = Stream(auth, l, timeout=60.0)
        print ('running...'+str(datetime.now()))
        
        #Only records 'locations' OR 'tracks', NOT 'tracks (keywords) with locations'
        while True:
            try:
                # Call tweepy's userstream method 
                # Use either locations or track, not both
                stream.filter(locations=[-11.0,48.0,3.1,63.0])##These coordinates are approximate bounding box
                #stream.filter(track=['holiday'])## This will feed the stream all mentions of 'keyword' 
                break
                
            except Exception as e:
                # Abnormal exit: Reconnect
                print ('error in loop - going for restart'+str(datetime.now()))
                print (e)
                nsecs=random.randint(5,10)
                time.sleep(nsecs)
                l=None
                stream=None
                author=None
                main()
                    

        l=None
        stream=None
        author=None
        main()

                 
    except Exception as e:
        print ('Major error - restarting')
        print (e)
        gc.collect()
        time.sleep(2)
        main()

    
if __name__ == '__main__':
    main()
    
    
    
    
