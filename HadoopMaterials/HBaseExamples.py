from starbase import Connection #starbase is default rest client and using connection object from it

c = Connection("127.0.0.1", "8000") # Using our ip address of our localhost and asking it to connect to the port specified on virtual box

ratings = c.table('ratings') #creating that schema

if (ratings.exists()):
    print("Dropping existing ratings table\n")
    ratings.drop()

ratings.create('rating') #within the ratings table create a column family named "rating"

print("Parsing the ml-100k ratings data...\n")
ratingFile = open("/Users/sourishr/Desktop/Big Data/Hadoop_Ecosystem_UDEMY/ml-100k/u.data", "r") #path to where the ml-data is stored on local and open it
#Instead of adding one row at a time, batch things up to make it efficient and do it all at once
batch = ratings.batch() #create batch object from ratings table

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}}) #'rating' column family is going to populate itself with a rating column of the movieID with a given rating value. So the column would be given by rating:movieID and the actual value in each cell is the rating itself

ratingFile.close()

print ("Committing ratings data to HBase via REST service\n")
batch.commit(finalize=True) #This saves the changes into HBase

print ("Get back ratings for some users...\n")
print ("Ratings for user ID 1:\n")
print (ratings.fetch("1")) #After this point we can actually start querying our data back
#Now as a client if I want to simulate all data and fetch them, it becomes possible. We can construct a webpage as well
print ("Ratings for user ID 33:\n")
print (ratings.fetch("33"))

ratings.drop()
