1 Project: 

The two data sets consist of tweet and user objects downloaded
from Twitter using Twitter API.

2 Data set
Each data set consists of two single JSON files downloaded using keyword search of Twitter’s standard search API. 
The tweets-xxx.json file contains around 10K tweet objects.

A tweet object may refer to a general tweet, a retweet or a reply to a tweet. Note that
reply or retweet could happen not only on general tweet, but also on other reply and/or
retweet. In other words, there could be reply to a retweet or a reply; and retweet of a reply.
In this project, we may refer to a tweet that receives a reply or retweet as a parent
tweet. This is not standard terminology used by Twitter or the general public; rather it is
a term borrowed from the tree data structure lexicon. The users-xxx.json file contains
corresponding user objects. The user object is referenced by user_id in the the tweet
object.


The common fields in all tweet objects are:
    • id: the unique id of the tweet.
    • created_at: the date and time the tweet is created.
    • text: the textual content of the tweet
    • user_id: the id of the user who created the tweet.

The optional fields in a tweet object are:
    • retweet_id: if the tweet is a retweet of another tweet, this field contains the parent
    tweet’s id.
    • retweet_user_id: if the tweet is a retweet of another tweet, this field contains the
    parent tweet’s user_id.
    • replyto_id: if the tweet is a reply to another tweet, this field contains the parent
    tweet’s id.
    • replyto_user_id: if the tweet is a reply to another tweet, this field contains the
    parent tweet’s user_id.
    • user_mentions: an array of {id, indices} showing the id of the users mentioned in
    the tweet text, as well as the location this user is mentioned.
    • hash_tags: an array of {tag, indices} showing the hash tag appearing in the tweet
    text and the location it appears.
    
    3 Query workload

    • [Q1] Find the number of general tweets with at least one reply and one retweet in the
    data set. Note that a general tweet is a tweet with neither a replyto_id field, nor a
    retweet_id field; a reply is a tweet with the replyto_id field; a retweet is a tweet
    with the retweet_id field.
    • [Q2] Find the reply tweet that has the most retweets in the data set.
    • [Q3] Find the top 5 hashtags appearing as the FIRST hashtag in a general or reply
    tweet, ignoring the case of the hashtag. Note that the order does not matter if a few
    hashtags have the same occurrence number.
    • [Q4] For a given hash_tag, there are many tweets including that hash_tag. Some
    of those tweets mention one or many users. Among all users mentioned in those
    tweets, find the top 5 users with the most followers_count. For each user, you
    should print out the id, name and location. Not all users have a profile in the users
    data set; you can ignore those that do not have a profile. If there are less than 5 users
    with profile, print just those users with a profile.
    • [Q5] Find the number of general tweets published by users with neither location nor
    description information.
    • [Q6] Find the general tweet that receives most retweets in the first hour after it is
    published. Print out the tweet Id and the number of retweets it received within the
    first hour.


4 Implementation Requirements
    Two json files will be imported into two collections called tweets and users respectively,
    belonging to a database called a1. These collections must not be updated in any subsequent queries and will be created before your mongo shell script is run (more details

    Implement each target workload as a SINGLE MongoDB find or aggregate command.In addition, you should provide an alternative implementation for Q4 and Q6. Each alternative implementation should be expressed as a SINGLE MongoDB find or aggregate
    command. It should have expressions that are substantively different to those in the original one. Changing only a query condition from "$exists:true" to "{$ne: null}" is not considered as a valid alternative implementation for this assignment. For aggregation,
    an alternative implementation may use different stages or arrange stages in a different
    order.

    Implementation and alternative implementation of all target workloads should be packaged as a single mongo shell script, which is a JavaScript file. The script should be executed by mongo shell in the form of mongo script.js --eval "args=’<hash_tag>’".
    The argument part is the input for Q4. A sample script a1sample.js is released along with
    the assignment instruction. This script assumes the practice data has been imported into
    the collections tweets and users in a database called a1. Your script should make the
    same assumption and can reuse the first few statements in the sample script to connect to
    the database server and to specify the database.

    At the end of the script, include command(s) to remove all collections you have created and return the database to the initial clean state with only the tweets and users collections.