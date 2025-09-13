const connect = require("./db");


const runDatabaseQueries = async () => {
  
  const db = await connect();
  const movies = db.collection('movies');


  // Get total count of movies
  const totalMovies = await movies.countDocuments();
  console.log('Total number of movies in database:', totalMovies);

  // Run this query, should get top 5 best rated movies on IMDB
  // const topMovies = await movies.find({ "imdb.rating": { $gt: 8.0 } })
  //   .project({ title: 1, year: 1, "imdb.rating": 1 })
  //   .sort({ "imdb.rating": -1 })
  //   .limit(5)
  //   .toArray();

  // console.log('Top Rated Movies:', topMovies);

  // // Run this query, should get all movies directed by George Lucas
  // const lucasMovies = await movies.find({ "directors": "George Lucas" })
  // .project({ title: 1, year: 1, description: 1 })
  // .toArray();

  // console.log('Movies Directed by George Lucas:', lucasMovies);

  // // Run this query, should get all movies directed by Christopher Nolan
  // const nolanMovies = await movies.find({ "directors": "Christopher Nolan" })
  //   .project({ title: 1, year: 1, description: 1 })
  //   .toArray();

  // console.log('Movies Directed by Christopher Nolan:', nolanMovies);

  // // Run this query, should get all movies with genre "Action" sorted by year descending
  // const actionMovies = await movies.find({ genres: "Action" })
  //   .project({ title: 1, year: 1, genres: 1 })
  //   .sort({ year: -1 })
  //   .toArray();

  // console.log('Action Movies:', actionMovies);

  // // Find movies with an IMDb rating greater than 8 and return only the title and IMDB information
  // const topRatedMovies = await movies.find({ "imdb.rating": { $gt: 8.0 } })
  //   .project({ title: 1, "imdb": 1 })
  //   .toArray();

  // console.log('Top Rated Movies:', topRatedMovies);
  // // Find movies that starred both "Tom Hanks" and "Tim Allen"
  // const tomAndTimMovies = await movies.find({ "cast": { $all: ["Tom Hanks", "Tim Allen"] } })
  //   .project({ title: 1, year: 1, cast: 1 })
  //   .toArray();

  // console.log('Movies that starred both Tom Hanks and Tim Allen:', tomAndTimMovies);

  // // Find movies that starred both and only "Tom Hanks" and "Tim Allen"
  // const tomAndTimOnlyMovies = await movies.find({ 
  //   $and: [
  //     { "cast": { $all: ["Tom Hanks", "Tim Allen"] } },
  //     { "cast": { $size: 2 } }
  //   ]
  // })
  //   .project({ title: 1, year: 1, cast: 1 })
  //   .toArray();

  // console.log('Movies that starred both Tom Hanks and Tim Allen only:', tomAndTimOnlyMovies);

  // Find comedy movies that are directed by Steven Spielberg
  const spielbergComedies = await movies.find({ 
    genres: "Comedy", 
    directors: "Steven Spielberg"
  })
    .project({ title: 1, year: 1, genres: 1, directors: 1 })
    .toArray();

  console.log('Comedy Movies Directed by Steven Spielberg:', spielbergComedies);

  // Calculate the average IMDb rating for movies grouped by director and display from highest to lowest.
  const avgRatingByDirector = await movies.aggregate([
    { $unwind: "$directors" },
    {
      $group: {
        _id: "$directors",
        avgRating: { $avg: "$imdb.rating" }
      }
    },
    { $sort: { avgRating: -1 } }
  ]).toArray();


  
  console.log('Average IMDb rating by director:', avgRatingByDirector);

  process.exit(0);
};


runDatabaseQueries();