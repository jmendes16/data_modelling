SELECT
    a.artist_name,
    COUNT(t.track_id) AS total_tracks
FROM
    artists AS a
JOIN
    tracks AS t ON a.artist_id = t.artist_id
GROUP BY
    a.artist_id, a.artist_name
ORDER BY
    total_tracks DESC
LIMIT 1;