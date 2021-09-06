import mysql.connector
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup

# HTTP request
r = requests.get("https://www.pythonhow.com/real-estate/rock-springs-wy/LCWYROCKSPRINGS", headers={'User-agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0'})
c=r.content

# Format data
soup=BeautifulSoup(c,'html.parser')
propertyRow=soup.find_all("div",{"class" : "propertyRow"})

# Get single perperty price and remove space
propertyRow[0].find("h4",{"class":"propPrice"}).text.replace("\n","").replace(" ","") 

l=[]

# Iterate thru each property
for item in propertyRow:
    d={}
    # Store details of each property
    d["Address"]=item.find_all("span", {"class","propAddressCollapse"})[0].text
    d["Locality"]=item.find_all("span", {"class","propAddressCollapse"})[1].text
    d["Price"]=item.find("h4",{"class","propPrice"}).text.replace("\n","").replace(" ","")
    
    # Handle None text exception
    try:
        d["Beds"]=item.find("span",{"class","infoBed"}).find("b").text
    except:
        d["Beds"]=None
    try:
        d["Area"]=item.find("span",{"class","infoSqFt"}).find("b").text
    except:
        d["Area"]=None
    try:
        d["Full Baths"]=item.find("span",{"class","infoValueFullBath"}).find("b").text
    except:
        d["Full Baths"]=None
    try:
        d["Half Baths"]=item.find("span",{"class","infoValueHalfBath"}).find("b").text
    except:
        d["Half Baths"]=None
    
    # More attributes
    for col_group in item.find_all("div",{"class":"columnGroup"}):
        for feature_group, feature_name in zip(col_group.find_all("span",{"class":"featureGroup"}),col_group.find_all("span",{"class":"featureName"})):
            if "Lot Size" in feature_group.text:
                d["Lot Size"]=feature_name.text
    
    l.append(d)


def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True

    dbcur.close()
    return False

##### Connect to mysql database ##### 
con = mysql.connector.connect(
host = "localhost",
user = "root",
password = "mysqlmysql",
database = "TestDB"
)
mycursor = con.cursor()

#####  Create engine ##### 
engine = create_engine('mysql://root:mysqlmysql@localhost/TestDB')
#####  Read CSV file ##### 
df = pd.read_csv("/Users/alexmeng/MyMac/coding/Projects/python-scraping/Real_estate_output.csv", sep = ',')

#####  Create Table ##### 
table = "RealEstate"
if (checkTableExists(con,table)):
    print("This table has already exists!")
else:
##### New table ##### 
# mycursor.execute('CREATE TABLE {tab} (ID int, Address nvarchar(30), Locality nvarchar(15), Price nvarchar(15), Beds int, Area int)'.format(tab=table))
##### Insert dataframes into mysql given table name ##### 
    df.to_sql(table, engine, index = False)

##### print Dataframes ##### 
#print("CSV file")
#print(df)

#####  Run SQL commands using mycursor.execute ##### 
#####  Aggregate Functions: SUM() AVG() COUNT() MIN() MAX()  ##### 
mycursor.execute("SELECT Price FROM RealEstate")
myresult = mycursor.fetchall()

for x in myresult:
    print(x)

#####  VISUALIZE MYSQL Data ##### 
#####  kind: line bar barh hist box kde density area pie scatter hexbin ##### 
##df1 = pd.read_sql("SELECT Address, Locality, Price, Beds,Area,Full Baths, Lot Size, FROM RealEstate", engine)
#df1.plot(kind = "density", x = "Price", y = "Area")
#plt.show()



# Refresh DB
con.commit()


