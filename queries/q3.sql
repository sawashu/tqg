SELECT max(c1.actor_id) FROM movie m1,
movie m2,
casting c1,
casting c2,
actor a1,
actor a2,
actor a3,
casting c3
WHERE m1.id = c1.movie_id
AND m2.id = c2.movie_id
AND c1.actor_id = a1.id
AND c2.actor_id = a2.id
AND a1.name like 'S%'
AND a1.id >= 40000
AND c2.ord = 2
AND m1.id = m2.id
AND m1.title like '% %'
AND m2.votes <= 50000
AND m1.director like '%c%'
AND a2.id <> a3.id
AND a3.name like '%d%'
AND c3.actor_id = a3.id
AND c3.movie_id > 100030;
