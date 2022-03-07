f_count = args
conn = new Mongo();
db = conn.getDB("a1");
var input = args
db.users.aggregate([{
		$project: {
			id: {
				$toString: "$id"
			},
			name: 1,
			location: 1,
			description: 1,
			followers_count: 1
		}
	},
	{
		$out: "users_2",
	},
]);
db.tweets.aggregate([{
		$project: {
			id: {
				$toString: "$id"
			},
			user_id: {
				$toString: "$user_id"
			},
			retweet_id: {
				$toString: "$retweet_id"
			},
			replyto_id: {
				$toString: "$replyto_id"
			},
			hash_tags: 1,
			user_mentions: 1,
			created_at: {
				$toDate: "$created_at"
			}
		}
	},
	{
		$out: "tweets_2",
	},
]);

//Create index
//Indexes for tweets_2
db.tweets_2.createIndex({
	"hash_tags.text": 1
}, {
	partialFilterExpression: {
		"hash_tags.text": {
			$exists: true
		}
	}
})
db.tweets_2.createIndex({
	retweet_id: 1
})
db.tweets_2.createIndex({
	replyto_id: 1
})

//Indexes for users_2
db.users_2.createIndex({
	id: 1
})
var start = new Date()

Q1 = db.tweets_2.aggregate([{
		$facet: {
			"retweet_count": [{
					$match: {
						retweet_id: {
							$ne: null
						}
					}
				},
				{
					$group: {
						_id: "$retweet_id",
						numOfRetweets: {
							$sum: 1
						}
					}
				},
				{
					$match: {
						numOfRetweets: {
							$gt: 0
						}
					}
				}
			],
			"reply_count": [{
					$match: {
						replyto_id: {
							$ne: null
						}
					}
				},
				{
					$group: {
						_id: "$replyto_id",
						numberOfReply: {
							$sum: 1
						}
					}
				},
				{
					$match: {
						numberOfReply: {
							$gt: 0
						}
					}
				}
			],
			"general": [{
				$match: {
					replyto_id: null,
					replytweew_id: null,
				}
			}, ]
		}
	},
	{
		$unwind: "$general"
	},
	{
		$project: {
			"general": 1,
			"retweet_count": {
				$filter: {
					input: "$retweet_count",
					as: "retweet_count",
					cond: {
						$eq: ["$$retweet_count._id", "$general.id"]
					}
				}
			},
			"reply_count": {
				$filter: {
					input: "$reply_count",
					as: "reply_count",
					cond: {
						$eq: ["$$reply_count._id", "$general.id"]
					}
				}
			},
		}
	},
	{
		$match: {
			"reply_count.0": {
				$exists: true
			},
			"retweet_count.0": {
				$exists: true
			}
		}
	},
	{
		$group: {
			_id: "$general_id",
			"Number of tweets": {
				$sum: 1
			}
		}
	},
	{
		$project: {
			_id: 0,
			"Number of tweets": 1
		}
	}
])


print("Q1 ====================")
// display the result
while (Q1.hasNext()) {
	printjson(Q1.next());
}

Q2 = db.tweets_2.aggregate([{
		$facet: {
			"retweet": [{
				$match: {
					retweet_id: {
						$ne: null
					}
				}
			}],
			"reply": [{
				$match: {
					replyto_id: {
						$ne: null
					}
				}
			}]
		}
	},
	{
		$unwind: {
			path: "$retweet"
		}
	},
	{
		$unwind: {
			path: "$reply"
		}
	},
	{
		$match: {
			$expr: {
				$eq: ["$retweet.retweet_id", "$reply.id"]
			}
		}
	},
	{
		$project: {
			id: "$reply.id"
		}
	},
	{
		$group: {
			_id: "$id",
			numOfRetweets: {
				$sum: 1
			}
		}
	},
	{
		$sort: {
			numOfRetweets: -1
		}
	},
	{
		$limit: 1
	}
])

print("Q2 ====================")
while (Q2.hasNext()) {
	printjson(Q2.next());
}

Q3 = db.tweets_2.aggregate([{
		$match: {
			$and: [{
				retweet_id: {
					$eq: null
				}
			}, {
				hash_tags: {
					$exists: true
				}
			}]
		}
	},
	{
		$project: {
			id: 1,
			user_id: 1,
			replyto_id: 1,
			min_hash_tags: {
				$min: "$hash_tags.indices"
			},
			hash_tags: 1
		}
	},
	{
		$unwind: "$hash_tags"
	},
	{
		$match: {
			$expr: {
				$eq: ["$hash_tags.indices", "$min_hash_tags"]
			}
		}
	},
	{
		$project: {
			first: "$hash_tags.text"
		}
	},
	{
		$project: {
			first: {
				$toLower: "$first"
			},
		}
	},
	{
		$group: {
			_id: "$first",
			count: {
				$sum: 1
			}
		}
	},
	{
		$sort: {
			count: -1
		}
	},
	{
		$limit: 5
	},
])
print("Q3 ====================")
while (Q3.hasNext()) {
	printjson(Q3.next());
}


Q4 = db.tweets_2.aggregate([{
		$match: {
			hash_tags: {
				$ne: null
			},
			user_mentions: {
				$ne: null
			}
		}
	},
	{
		$unwind: "$hash_tags"
	},
	{
		$project: {
			"hash_tags": "$hash_tags.text",
			"mentioned_user": "$user_mentions.id"
		}
	},
	{
		$match: {
			"hash_tags": {
				$eq: input
			}
		}
	},
	{
		$unwind: "$mentioned_user"
	},
	{
		$project: {
			mentioned_user: {
				$toString: "$mentioned_user"
			}
		}
	},
	{
		$group: {
			_id: "$mentioned_user"
		}
	},
	{
		$lookup: {
			from: "users_2",
			localField: "_id",
			foreignField: "id",
			as: "mention_user_list"
		}
	},
	{
		$match: {
			mention_user_list: {
				$not: {
					$size: 0
				}
			}
		}
	},
	{
		$sort: {
			"mention_user_list.followers_count": -1
		}
	},
	{
		$project: {
			_id: 1,
			name: "$mention_user_list.name",
			location: "$mention_user_list.location",
			follower_count: "$mention_user_list.followers_count"
		}
	},
	{
		$unwind: "$name"
	},
	{
		$unwind: "$location"
	},
	{
		$unwind: "$follower_count"
	},
	{
		$limit: 5
	}
])
print("Q4 ====================")
while (Q4.hasNext()) {
	printjson(Q4.next());

}

Q5 = db.tweets_2.aggregate([{
		$match: {
			retweet_id: {
				$eq: null
			},
			replyto_id: {
				$eq: null
			}
		}
	},
	{
		$lookup: {
			from: "users_2",
			localField: "user_id",
			foreignField: "id",
			as: "user_profile"
		}
	},
	{
		$match: {
			$and: [{
					"user_profile.description": {
						$eq: ""
					}
				},
				{
					"user_profile.location": {
						$eq: ""
					}
				}
			]
		}
	},
	{
		$count: "id"
	},
	{
		$project: {
			tweet_count: "$id"
		}
	}
])
print("Q5 ====================")
while (Q5.hasNext()) {
	printjson(Q5.next());
}

Q6 = db.tweets_2.aggregate([{
		$facet: {
			"retweet": [{
					$match: {
						retweet_id: {
							$ne: null
						}
					}
				},
				{
					$group: {
						_id: "$retweet_id",
						retweets: {
							$push: "$created_at"
						},
						retweet_count: {
							$sum: 1
						}
					}
				},
			],
			"general": [{
				$match: {
					replyto_id: {
						$eq: null
					},
					retweet_id: {
						$eq: null
					}
				}
			}]
		}
	},
	{
		$unwind: "$general"
	},
	{
		$unwind: "$retweet"
	},
	{
		$match: {
			$expr: {
				$eq: ["$general.id", "$retweet._id"]
			}
		}
	},
	{
		$project: {
			"retweet_time": "$retweet.retweets",
			"original_tweet_time": "$general.created_at",
			"id": "$general.id"
		}
	},
	{
		$unwind: "$retweet_time"
	},
	{
		$project: {
			"id": 1,
			"time_difference": {
				$subtract: ["$retweet_time", "$original_tweet_time"]
			}
		}
	},
	{
		$match: {
			"time_difference": {
				$lte: 3600000
			}
		}
	},
	{
		$group: {
			_id: "$id",
			"Number of tweets": {
				$sum: 1
			}
		}
	},
	{
		$sort: {
			"Number of tweets": -1
		}
	},
	{
		$limit: 1
	}
])

print("Q6 ====================")
while (Q6.hasNext()) {
	printjson(Q6.next());
}

Q4_alternative = db.users_2.aggregate([{
		$lookup: {
			from: "tweets_2",
			let: {
				profile_user_id: "$id"
			},
			pipeline: [{
					$match: {
						"user_mentions": {
							$exists: true
						},
						"hash_tags": {
							$exists: true
						},
					}
				},
				{
					$unwind: "$user_mentions"
				},
				{
					$project: {
						"id": 1,
						"hash_tags": "$hash_tags.text",
						"user_mentions_id": {
							$toString: "$user_mentions.id"
						}
					}
				},
				{
					$match: {
						$expr: {
							$eq: ["$user_mentions_id", "$$profile_user_id"]
						}
					}
				}
			],
			as: "matches"
		}
	},
	{
		$match: {
			"matches.0": {
				$exists: true
			}
		}
	},
	{
		$unwind: "$matches"
	},
	{
		$project: {
			"id": 1,
			"name": 1,
			"followers_count": 1,
			"hash_tags": "$matches.hash_tags",
			"location": 1
		}
	},
	{
		$match: {
			"hash_tags": {
				$eq: input
			}
		}
	},
	{
		$group: {
			_id: "$id",
			"followers_count": {
				$push: "$followers_count"
			},
			"name": {
				$push: "$name"
			},
			"location": {
				$push: "$location"
			}
		}
	},
	{
		$sort: {
			"followers_count": -1
		}
	},
	{
		$project: {
			"_id": 1,
			"followersCount": {
				$first: "$followers_count"
			},
			"name": {
				$first: "$name"
			},
			"location": {
				$first: "$location"
			}
		}
	},
	{
		$limit: 5
	}
])

print("Q4_alternative ====================")
while (Q4_alternative.hasNext()) {
	printjson(Q4_alternative.next());
}

Q6_alternative = db.tweets_2.aggregate([{
		$lookup: {
			from: "tweets_2",
			localField: "id",
			foreignField: "retweet_id",
			as: "retweet_list"
		}
	},
	{
		$match: {
			"retweet_list.0": {
				$exists: 1
			},
			"retweet_id": {
				$eq: null
			},
			"replyto_id": {
				$eq: null
			}
		}
	},
	{
		$unwind: "$retweet_list"
	},
	{
		$addFields: {
			time_difference: {
				$subtract: ["$retweet_list.created_at", "$created_at"]
			},
		}
	}, {
		$match: {
			"time_difference": {
				$lte: 3600000
			}
		}
	},
	{
		$group: {
			_id: "$id",
			"Number of retweets": {
				$sum: 1
			}
		}
	},
	{
		$sort: {
			"Number of retweets": -1
		}
	},
	{
		$limit: 1
	}
])
print("Q6_alternative ====================")
while (Q6_alternative.hasNext()) {
	printjson(Q6_alternative.next());
}

var end = new Date()
print("Execution time: " + (end - start) + "ms")
db.tweets_2.drop()
db.users_2.drop()