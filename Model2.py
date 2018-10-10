from datetime import datetime
import csv
from pprint import pprint

from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Sequence
from sqlalchemy import DateTime
from sqlalchemy import Boolean
from sqlalchemy import MetaData
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy import insert


from sqlalchemy_utils import IPAddressType

class Modems:

    def __init__(self):
        self.devices = []
        #self.text = 'mod2.text'
        #self.text = 'modemipadd.csv'
        self.results = []
        
    def getModems(self):
        '''Gets a list of modem connections to add from a text file.

        Expects a text file with 
        <name>,<ip address>
        per line to parse through for connections to test.
        These are placed into self.devices

        '''
        with open(self.text, 'r') as modemFile:
            modReader = csv.reader(modemFile)
            for row in modReader:
                self.devices.append(row)

#if __name__ == "__main__":
metadata = MetaData()

modems = Table('modems', metadata,
               Column('modem_id', Integer(), primary_key=True),
               Column('location_id', ForeignKey('locations.location_id'),
                      nullable=False),
               Column('client_id', ForeignKey('clients.client_id'),
                      nullable=False),
               Column('modem_ip',IPAddressType, nullable=False)
)
    
locations = Table('locations', metadata,
                  Column('location_id', Integer(), primary_key=True),
                  Column('location_name', String(255), nullable=False),
                  Column('client_id', ForeignKey('clients.client_id'),
                         nullable=False)
)
     
poll = Table('poll', metadata,
             Column('poll_id', Integer(), primary_key=True),
             Column('modem_id', ForeignKey('modems.modem_id'),nullable=False),
             Column('poll_dt', DateTime(), default=datetime.now,
                    nullable=False),
             Column('status', Boolean(), nullable=False)
)
    
clients = Table('clients', metadata,
                Column('client_id', Integer(), primary_key=True),
                Column('client_name', String(255), unique=True)
)

engine = create_engine('sqlite:///db_monitor.db')

metadata.create_all(engine)
    
connection = engine.connect() #make actual connection to DB engine
def uglyModemInsert(csvfile):
    """ Creates correct entries in the modems table, poorly
    csvfile = csv file with the format: client name, location name, location IP
    Reads location_name from locations
    Reads client_name, client_id from clients
    Reads in csvfile
    matches results from DB to client_name from csvfile and appends IP then
    gets the new_results ready to insert into modems table
    

    """
    columns = [locations.c.location_name, clients.c.client_name, clients.c.client_id]
    client_location = select(columns)
    client_location = client_location.select_from(locations.join(clients))
    results = connection.execute(client_location).fetchall()
    
    results_dic = {'data' : []}
    #pprint(results) #***
    for result in results:
	d = {}
	d['location_name'] = result[0]
	d['client_name'] = result[1]
	d['client_id'] = result[2]
	results_dic['data'].append(d)
	    
		    
    modem_data = []
    with open(csvfile, 'r') as modemFile:
	modReader = csv.reader(modemFile)
	for row in modReader:
	    temp_dic = {}
	    temp_dic['location_name'] = row[1]
	    temp_dic['client_name'] = row[0]
	    temp_dic['modem_ip'] = row[2]
	    modem_data.append(temp_dic)
	        
	        
    new_results = {'data' : []}
    for entry in results_dic["data"]:
	d = {}
	d = entry
	for x in modem_data:
	    if x['location_name'] == entry['location_name']:
	        d['location_name'] = x['location_name']
	        d['modem_ip'] = x['modem_ip']
	        del d['client_name']
	new_results['data'].append(d)
	
    ins = modems.insert()
    result = connection.execute(ins, new_results['data'])

