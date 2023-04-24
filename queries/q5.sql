SELECT COUNT(*) FROM movie m1,
movie m2,
casting c1,
casting c2,
actor a1,
actor a2,
movie m3,
movie m4
WHERE m1.id = c1.movie_id
AND m2.id = c2.movie_id
AND c1.actor_id = a1.id
AND c2.actor_id = a2.id
AND a1.name like 'A%'
AND c2.ord >= 2
AND m1.id = m2.id
AND m1.yr > 2000
AND m2.votes >= 350000
AND m1.director not like '%h%'
AND m3.title like '%f%'
AND m1.id <> m3.id
AND m3.score = 8.0;

