db.tracks.aggregate([
  // Stage 1: Group documents by artist ID and get a count
  {
    $group: {
      _id: {
        artist_id: "$artist.artist_id",
        artist_name: "$artist.artist_name"
      },
      total_tracks: { $sum: 1 }
    }
  },
  // Stage 2: Sort by the new total_tracks field in descending order
  {
    $sort: {
      total_tracks: -1
    }
  },
  // Stage 3: Limit to the single top result
  {
    $limit: 1
  },
  // Stage 4 (Optional): Clean up the output format
  {
    $project: {
      _id: 0,
      artist_name: "$_id.artist_name",
      artist_id: "$_id.artist_id",
      total_tracks: "$total_tracks"
    }
  }
])

db.artists.aggregate([
  // Stage 1: Filter for "Bryan Baker" ONLY
  {
    $match: {
      artist_name: "Bryan Baker"
    }
  },
  // Stage 2: Deconstruct the 'albums' array
  {
    $unwind: "$albums"
  },
  // Stage 3: Deconstruct the 'tracks' array
  {
    $unwind: "$albums.tracks"
  },
  // Stage 4: Group and count the tracks for the artist
  {
    $group: {
      _id: { 
        artist_id: "$_id", 
        artist_name: "$artist_name" 
      },
      total_tracks: { $sum: 1 }
    }
  }
])